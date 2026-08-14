#include "Http.hpp"
#include "Logger.hpp"
#include "misc.hpp"
#include "version.hpp"

#include <boost/asio/system_timer.hpp>
#include <boost/beast/version.hpp>

namespace
{
	template <class StringView>
	std::string StringViewToString(StringView const &value)
	{
		return std::string(value.data(), value.size());
	}
}

Http::Http(std::string token) :
	m_SslContext(asio::ssl::context::tlsv12_client),
	m_Token(token),
	m_NetworkThreadRunning(true),
	m_NetworkThread(std::bind(&Http::NetworkThreadFunc, this))
{

}

Http::~Http()
{
	m_NetworkThreadRunning = false;
	m_NetworkThread.join();

	// drain requests queue
	QueueEntry *entry;
	while (m_Queue.pop(entry))
		delete entry;
}

void Http::AddBucketIdentifierFromURL(std::string url, std::string bucket)
{
	std::string reduced_url = "";
	// Remove IDs from the string
	while (url.find("/") != std::string::npos)
	{
		std::string part = url.substr(0, url.find("/", 1));

		if (part.find_first_of("0123456789") == std::string::npos)
		{
			reduced_url += part;
		}

		url.erase(0, url.find("/", 1));
	}

	bucket_urls[reduced_url] = std::move(bucket);
}

std::string const Http::GetBucketIdentifierFromURL(std::string url)
{
	std::string reduced_url = "";
	// Remove IDs from the string
	while (url.find("/") != std::string::npos)
	{
		std::string part = url.substr(0, url.find("/", 1));

		if (part.find_first_of("0123456789") == std::string::npos)
		{
			reduced_url += part;
		}

		url.erase(0, url.find("/", 1));
	}

	if (bucket_urls.find(reduced_url) != bucket_urls.end())
	{
		return bucket_urls.at(reduced_url);
	}
	return "INVALID";
}

void Http::NetworkThreadFunc()
{
	unsigned int retry_counter = 0;
	unsigned int const MaxRetries = 3;
	bool skip_entry = false;
	std::unordered_map<std::string, TimePoint_t> bucket_ratelimit;
	TimePoint_t global_ratelimit{};

	if (!Connect())
		return;

	while (m_NetworkThreadRunning)
	{
		TimePoint_t current_time = std::chrono::steady_clock::now();
		std::list<QueueEntry*> skipped_entries;
		QueueEntry *entry;
		while (m_Queue.pop(entry))
		{
			if (current_time < global_ratelimit)
			{
				skipped_entries.push_back(entry);
				continue;
			}

			// check if we're rate-limited
			std::string bucket = GetBucketIdentifierFromURL(StringViewToString(entry->Request->target()));
			auto pr_it = bucket_ratelimit.find(bucket);
			if (pr_it != bucket_ratelimit.end() && bucket != "INVALID")
			{
				// rate-limit for this bucket exists
				// are we still within the rate-limit timepoint?
				if (current_time < pr_it->second)
				{
					// yes, ignore this request for now
					skipped_entries.push_back(entry);
					continue;
				}

				// no, delete rate-limit and go on
				bucket_ratelimit.erase(pr_it);
				Logger::Get()->Log(samplog_LogLevel::DEBUG, "rate-limit on bucket '{}' lifted",
					StringViewToString(entry->Request->target()));
			}

			boost::system::error_code error_code;
			Response_t response;
			Streambuf_t sb;
			retry_counter = 0;
			skip_entry = false;
			bool requeue_entry = false;
			do
			{
				if (retry_counter > 0)
				{
					response = Response_t{};
					sb.consume(sb.size());
					error_code.clear();
				}

				bool do_reconnect = false;
				beast::http::write(*m_SslStream, *entry->Request, error_code);
				if (error_code)
				{
					Logger::Get()->Log(samplog_LogLevel::ERROR, "Error while sending HTTP {} request to '{}': {}",
						StringViewToString(entry->Request->method_string()),
						StringViewToString(entry->Request->target()),
						error_code.message());

					do_reconnect = true;
				}
				else
				{
					beast::http::read(*m_SslStream, sb, response, error_code);
					if (error_code)
					{
						Logger::Get()->Log(samplog_LogLevel::ERROR, "Error while retrieving HTTP {} response from '{}': {}",
							StringViewToString(entry->Request->method_string()),
							StringViewToString(entry->Request->target()),
							error_code.message());

						do_reconnect = true;
					}
					else if (response.result_int() == 429 /* rate limited */)
					{
						double retry_after_seconds = 1.0;
						auto retry_after = response.find("Retry-After");
						if (retry_after != response.end())
						{
							try
							{
								retry_after_seconds = std::stod(StringViewToString(retry_after->value()));
							}
							catch (std::exception const &)
							{
								// Keep the conservative one-second fallback.
							}
						}

						auto retry_delay = std::chrono::milliseconds(
							static_cast<long long>(retry_after_seconds * 1000.0) + 250);
						auto response_bucket = response.find("X-RateLimit-Bucket");
						if (response_bucket != response.end())
							AddBucketIdentifierFromURL(StringViewToString(entry->Request->target()),
								StringViewToString(response_bucket->value()));
						bucket = GetBucketIdentifierFromURL(StringViewToString(entry->Request->target()));
						auto global_header = response.find("X-RateLimit-Global");
						bool is_global = global_header != response.end()
							&& global_header->value() == "true";
						if (is_global)
							global_ratelimit = std::chrono::steady_clock::now() + retry_delay;
						else
							bucket_ratelimit[bucket] = std::chrono::steady_clock::now() + retry_delay;
						Logger::Get()->Log(samplog_LogLevel::WARNING,
							"Discord rate-limited path '{}'; retrying in {} ms",
							StringViewToString(entry->Request->target()), retry_delay.count());
						skipped_entries.push_back(entry);
						requeue_entry = true;
						skip_entry = true;
						break;
					}
				}

				if (do_reconnect)
				{
					if (retry_counter++ >= MaxRetries || !ReconnectRetry())
					{
						// we failed to reconnect, discard this request
						Logger::Get()->Log(samplog_LogLevel::WARNING, "Failed to send request, discarding");
						skip_entry = true;
						break; // break out of do-while loop
					}
				}
			} while (error_code);
			if (skip_entry)
			{
				if (!requeue_entry)
					delete entry;
				continue; // continue queue loop
			}

			auto it_r = response.find("X-RateLimit-Remaining");
			if (it_r != response.end())
			{
				auto bucket_identifier = response.find("X-RateLimit-Bucket");
				if (bucket_identifier != response.end())
				{
					if (bucket_urls.find(StringViewToString(bucket_identifier->value())) == bucket_urls.end())
					{
						//Logger::Get()->Log(samplog_LogLevel::ERROR, "{}", entry->Request->target().to_string());
						AddBucketIdentifierFromURL(StringViewToString(entry->Request->target()), StringViewToString(bucket_identifier->value()));
					}
				}

				bucket = GetBucketIdentifierFromURL(StringViewToString(entry->Request->target()));
				if (it_r->value().compare("0") == 0)
				{
					// we're now officially rate-limited
					// the next call to this path will fail
					auto lit = bucket_ratelimit.find(bucket);
					if (lit != bucket_ratelimit.end())
					{
						Logger::Get()->Log(samplog_LogLevel::ERROR,
							"Error while processing rate-limit: already rate-limited bucket '{}'",
							bucket);

						// skip this request, we'll re-add it to the queue to retry later
						skipped_entries.push_back(entry);
						continue;
					}

					it_r = response.find("X-RateLimit-Reset-After");
					if (it_r != response.end())
					{
						string const reset_time_str = StringViewToString(it_r->value());
						double reset_after_seconds = 1.0;
						try
						{
							reset_after_seconds = std::stod(reset_time_str);
						}
						catch (std::exception const &)
						{
							// Keep the conservative one-second fallback.
						}
						std::chrono::milliseconds milliseconds(
							static_cast<long long>(reset_after_seconds * 1000.0));

						std::chrono::milliseconds timepoint_now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
						Logger::Get()->Log(samplog_LogLevel::DEBUG, "rate-limiting bucket {} until {} (current time: {})",
							bucket,
							StringViewToString(it_r->value()),
							timepoint_now.count());
						TimePoint_t reset_time = std::chrono::steady_clock::now()
							+ std::chrono::milliseconds(milliseconds.count() + 250); // add a buffer of 250 ms

						bucket_ratelimit.insert({ bucket, reset_time });
					}
				}
			}

			if (entry->Callback)
				entry->Callback(sb, response);

			delete entry;
			entry = nullptr;
		}

		// add skipped entries back to queue
		for (auto e : skipped_entries)
		{
			if (!m_Queue.push(e))
			{
				Logger::Get()->Log(samplog_LogLevel::ERROR,
					"HTTP request queue is full; discarding deferred request");
				delete e;
			}
		}

		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	Disconnect();
}

bool Http::Connect()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Connect");

	m_SslStream.reset(new SslStream_t(m_IoService, m_SslContext));

	const char *API_HOST = "discord.com";

	// Set SNI Hostname (many hosts need this to handshake successfully)
	if (!SSL_set_tlsext_host_name(m_SslStream->native_handle(), API_HOST))
	{
		beast::error_code ec{ 
			static_cast<int>(::ERR_get_error()),
			asio::error::get_ssl_category() };
		Logger::Get()->Log(samplog_LogLevel::ERROR,
			"Can't set SNI hostname for Discord API URL: {} ({})",
			ec.message(), ec.value());
		return false;
	}

	// connect to REST API
	asio::ip::tcp::resolver r{ m_IoService };
	boost::system::error_code error;
	auto target = r.resolve(API_HOST, "443", error);
	if (error)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR, "Can't resolve Discord API URL: {} ({})",
			error.message(), error.value());
		return false;
	}

	beast::get_lowest_layer(*m_SslStream).expires_after(std::chrono::seconds(30));
	beast::get_lowest_layer(*m_SslStream).connect(target, error);
	if (error)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR, "Can't connect to Discord API: {} ({})",
			error.message(), error.value());
		return false;
	}

	// SSL handshake
	m_SslStream->handshake(asio::ssl::stream_base::client, error);
	if (error)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR, "Can't establish secured connection to Discord API: {} ({})",
			error.message(), error.value());
		return false;
	}

	return true;
}

void Http::Disconnect()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Disconnect");

	boost::system::error_code error;
	m_SslStream->shutdown(error);
	if (error && error != boost::asio::error::eof && error != boost::asio::ssl::error::stream_truncated)
	{
		Logger::Get()->Log(samplog_LogLevel::WARNING, "Error while shutting down SSL on HTTP connection: {} ({})",
			error.message(), error.value());
	}
}

bool Http::ReconnectRetry()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::ReconnectRetry");

	unsigned int reconnect_counter = 0;
	do
	{
		Logger::Get()->Log(samplog_LogLevel::INFO, "trying reconnect #{}...", reconnect_counter + 1);

		Disconnect();
		if (Connect())
		{
			Logger::Get()->Log(samplog_LogLevel::INFO, "reconnect succeeded, resending request");
			return true;
		}
		else
		{
			unsigned int seconds_to_wait = static_cast<unsigned int>(std::pow(2U, reconnect_counter));
			Logger::Get()->Log(samplog_LogLevel::WARNING, "reconnect failed, waiting {} seconds...", seconds_to_wait);
			std::this_thread::sleep_for(std::chrono::seconds(seconds_to_wait));
		}
	} while (++reconnect_counter < 3);

	Logger::Get()->Log(samplog_LogLevel::ERROR, "Could not reconnect to Discord");
	return false;
}

Http::SharedRequest_t Http::PrepareRequest(beast::http::verb const method,
	std::string const &url, std::string const &content, bool use_api)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::PrepareRequest");

	auto req = std::make_shared<Request_t>();
	req->method(method);
	req->target((use_api == true ? "/api/v10" : "") + url);
	req->version(11);
	req->insert(beast::http::field::connection, "keep-alive");
	req->insert(beast::http::field::host, "discord.com");
	req->insert(beast::http::field::user_agent,
		"samp-discord-connector (" BOOST_BEAST_VERSION_STRING ")");
	if (!content.empty())
		req->insert(beast::http::field::content_type, "application/json");
	req->insert(beast::http::field::authorization, "Bot " + m_Token);
	req->body() = content;

	req->prepare_payload();

	return req;
}

void Http::SendRequest(beast::http::verb const method, std::string const &url,
	std::string const &content, ResponseCallback_t &&callback, bool use_api)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::SendRequest");

	SharedRequest_t req = PrepareRequest(method, url, content, use_api);

	if (callback == nullptr && Logger::Get()->IsLogLevel(samplog_LogLevel::DEBUG))
	{
		callback = CreateResponseCallback([=](Response r)
		{
			Logger::Get()->Log(samplog_LogLevel::DEBUG, "{:s} {:s} [{:s}] --> {:d} {:s}: {:s}",
				StringViewToString(beast::http::to_string(method)), url, content, r.status, r.reason, r.body);
		});
	}

	auto entry = new QueueEntry(req, std::move(callback));
	if (!m_Queue.push(entry))
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR,
			"HTTP request queue is full; discarding request to '{}'", url);
		delete entry;
	}
}

Http::ResponseCallback_t Http::CreateResponseCallback(ResponseCb_t &&callback)
{
	if (callback == nullptr)
		return nullptr;

	return [callback](Streambuf_t &sb, Response_t &resp)
	{
		callback({ resp.result_int(), StringViewToString(resp.reason()),
			beast::buffers_to_string(resp.body().data()), beast::buffers_to_string(sb.data()) });
	};
}


void Http::Get(std::string const &url, ResponseCb_t &&callback, bool use_api)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Get");

	SendRequest(beast::http::verb::get, url, "",
		CreateResponseCallback(std::move(callback)), use_api);
}

void Http::Post(std::string const &url, std::string const &content,
	ResponseCb_t &&callback /*= nullptr*/)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Post");

	SendRequest(beast::http::verb::post, url, content,
		CreateResponseCallback(std::move(callback)));
}

void Http::Delete(std::string const &url)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Delete");

	SendRequest(beast::http::verb::delete_, url, "", nullptr);
}

void Http::Put(std::string const &url, std::string const& content)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Put");

	SendRequest(beast::http::verb::put, url, content, nullptr);
}

void Http::Patch(std::string const &url, std::string const &content)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "Http::Patch");

	SendRequest(beast::http::verb::patch, url, content, nullptr);
}
