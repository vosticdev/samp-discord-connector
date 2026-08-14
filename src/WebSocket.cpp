#include "WebSocket.hpp"
#include "Logger.hpp"
#include "sdk.hpp"

#include <unordered_map>
#include <random>

extern logprintf_t logprintf;

WebSocket::WebSocket() :
	_ioContext(),
	_resolver(asio::make_strand(_ioContext)),
	_sslContext(asio::ssl::context::tlsv12_client),
	_reconnectTimer(_ioContext),
	m_HeartbeatTimer(_ioContext),
	m_HeartbeatInterval(),
	m_PresenceTimer(_ioContext)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::WebSocket");
}

WebSocket::~WebSocket()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::~WebSocket");

	m_ShuttingDown = true;
	Disconnect();

	if (_netThread)
		_netThread->join();
}

void WebSocket::Initialize(std::string token, std::string gateway_url, int intents)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::Initialize");

	_gatewayUrl = gateway_url;
	_apiToken = token;
	_intents = intents;
	Connect();

	_netThread = std::make_unique<std::thread>([this]()
	{
		_ioContext.run();
	});
}

void WebSocket::Connect()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::Connect");
	m_ConnectionState = ConnectionState::CONNECTING;
	m_CloseRequested = false;
	m_CloseInProgress = false;
	m_ReconnectScheduled = false;
	m_ConnectingGatewayUrl = m_ShouldResume && !m_ResumeGatewayUrl.empty()
		? m_ResumeGatewayUrl : _gatewayUrl;

	_resolver.async_resolve(
		m_ConnectingGatewayUrl,
		"443",
		beast::bind_front_handler(
			&WebSocket::OnResolve,
			this));
}

void WebSocket::OnResolve(beast::error_code ec, 
	asio::ip::tcp::resolver::results_type results)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::OnResolve");

	if (ec)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR, 
			"Can't resolve Discord gateway URL '{}': {} ({})",
			_gatewayUrl, ec.message(), ec.value());
		Disconnect(true);
		return;
	}

	_websocket.reset(
		new WebSocketStream_t(asio::make_strand(_ioContext), _sslContext));

	beast::get_lowest_layer(*_websocket).expires_after(
		std::chrono::seconds(30));
	beast::get_lowest_layer(*_websocket).async_connect(
		results, 
		beast::bind_front_handler(
			&WebSocket::OnConnect,
			this));
}

void WebSocket::OnConnect(beast::error_code ec,
	asio::ip::tcp::resolver::results_type::endpoint_type ep)
{
	boost::ignore_unused(ep);

	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::OnConnect");

	if (ec)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR, 
			"Can't connect to Discord gateway: {} ({})",
			ec.message(), ec.value());
		Disconnect(true);
		return;
	}

	beast::get_lowest_layer(*_websocket).expires_after(std::chrono::seconds(30));
	_websocket->next_layer().async_handshake(
		asio::ssl::stream_base::client, 
		beast::bind_front_handler(
			&WebSocket::OnSslHandshake,
			this));
}

void WebSocket::OnSslHandshake(beast::error_code ec)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::OnSslHandshake");

	if (ec)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR,
			"Can't establish secured connection to Discord gateway: {} ({})",
			ec.message(), ec.value());
		Disconnect(true);
		return;
	}

	// websocket stream has its own timeout system
	beast::get_lowest_layer(*_websocket).expires_never();

	_websocket->set_option(
		beast::websocket::stream_base::timeout::suggested(
			beast::role_type::client));

	// set a decorator to change the User-Agent of the handshake
	_websocket->set_option(beast::websocket::stream_base::decorator(
		[](beast::websocket::request_type &req)
	{
		req.set(beast::http::field::user_agent,
			std::string(BOOST_BEAST_VERSION_STRING) +
			" samp-discord-connector");
	}));

	_websocket->async_handshake(
		m_ConnectingGatewayUrl + ":443",
		"/?encoding=json&v=10",
		beast::bind_front_handler(
			&WebSocket::OnHandshake,
			this));
}

void WebSocket::OnHandshake(beast::error_code ec)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::OnHandshake");

	if (ec)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR,
			"Can't upgrade to WSS protocol: {} ({})",
			ec.message(), ec.value());
		Disconnect(true);
		return;
	}

	m_ConnectionState = ConnectionState::AWAITING_HELLO;

	// Discord requires HELLO before IDENTIFY or RESUME.
	Read();
}

void WebSocket::Disconnect(bool reconnect /*= false*/)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::Disconnect");

	asio::post(_ioContext, [this, reconnect]()
	{
		if (!reconnect)
		{
			m_ReconnectAfterClose = false;
			_reconnectTimer.cancel();
		}
		else if (!m_ShuttingDown)
		{
			m_ReconnectAfterClose = true;
		}
		m_CloseRequested = true;
		m_HeartbeatTimer.cancel();

		// async_close is a write operation. Wait for the active user write first.
		if (m_WriteQueue.empty())
			BeginClose();
	});
}

void WebSocket::BeginClose()
{
	if (!m_CloseRequested || m_CloseInProgress)
		return;
	if (!_websocket)
	{
		m_CloseRequested = false;
		OnClose({});
		return;
	}

	m_CloseRequested = false;
	m_CloseInProgress = true;
	if (_websocket->is_open())
	{
		beast::websocket::close_reason reason;
		reason.code = m_ReconnectAfterClose
			? static_cast<beast::websocket::close_code>(4000)
			: beast::websocket::close_code::normal;
		_websocket->async_close(
			reason,
			beast::bind_front_handler(&WebSocket::OnClose, this));
	}
	else
	{
		OnClose({});
	}
}

void WebSocket::OnClose(beast::error_code ec)
{
	boost::ignore_unused(ec);

	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::OnClose");

	m_CloseInProgress = false;
	m_HeartbeatTimer.cancel();
	m_PresenceTimer.cancel();
	m_PresenceTimerScheduled = false;
	m_ConnectionState = ConnectionState::DISCONNECTED;
	m_AwaitingHeartbeatAck = false;
	m_WriteQueue.clear();

	if (m_ReconnectAfterClose && !m_ShuttingDown)
		ScheduleReconnect();
}

void WebSocket::ScheduleReconnect()
{
	if (m_ReconnectScheduled || m_ShuttingDown)
		return;

	m_ReconnectScheduled = true;
	m_ReconnectAfterClose = false;
	++_reconnectCount;

	// Capped exponential backoff with jitter prevents reconnect storms.
	unsigned int exponent = std::min(_reconnectCount, 6u);
	unsigned int maximum = std::min(1u << exponent, 60u);
	std::random_device rd;
	std::uniform_int_distribution<unsigned int> jitter(maximum / 2, maximum);
	auto delay = std::chrono::seconds(jitter(rd));

	Logger::Get()->Log(samplog_LogLevel::INFO,
		"reconnecting in {:d} seconds", delay.count());
	_reconnectTimer.expires_after(delay);
	_reconnectTimer.async_wait(
		beast::bind_front_handler(&WebSocket::OnReconnect, this));
}

void WebSocket::OnReconnect(beast::error_code ec)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::OnReconnect");

	if (ec)
	{
		switch (ec.value())
		{
		case boost::asio::error::operation_aborted:
			// timer was chancelled, do nothing
			Logger::Get()->Log(samplog_LogLevel::DEBUG, "reconnect timer chancelled");
			break;
		default:
			Logger::Get()->Log(samplog_LogLevel::ERROR, "reconnect timer error: {} ({})",
				ec.message(), ec.value());
			break;
		}
		return;
	}

	m_ReconnectScheduled = false;
	Connect();
}

void WebSocket::Read()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::Read");

	_websocket->async_read(
		_buffer,
		beast::bind_front_handler(
			&WebSocket::OnRead,
			this));
}

void WebSocket::OnRead(beast::error_code ec,
	std::size_t bytes_transferred)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, 
		"WebSocket::OnRead({:d})",
		bytes_transferred);

	if (ec)
	{
		bool reconnect = false;
		if (ec == beast::websocket::error::closed
			|| ec == asio::ssl::error::stream_errors::stream_truncated)
		{
			auto close_code = static_cast<unsigned int>(_websocket->reason().code);
			Logger::Get()->Log(samplog_LogLevel::ERROR,
				"Discord terminated websocket connection; reason: {} ({})",
				_websocket->reason().reason.c_str(),
				close_code);

			switch (close_code)
			{
			case 4003: // not authenticated / invalidated session
			case 4005: // already authenticated
			case 4007: // invalid sequence
			case 4009: // session timed out
			case 1000: // normal close invalidates the session
			case 1001:
				ResetSession();
				reconnect = true;
				break;
			case 4004: // authentication failed
			case 4010: // invalid shard
			case 4011: // sharding required
			case 4012: // invalid API version
			case 4013: // invalid intents
			case 4014: // disallowed intents
				reconnect = false;
				break;
			default:
				reconnect = true;
				break;
			}

			if (close_code == 4014)
			{
				logprintf(" >> discord-connector: bot could not connect due to intent permissions. Modify your discord bot settings and enable every intent.");
			}
		}
		else if (ec == asio::error::operation_aborted)
		{
			// connection was closed, do nothing
		}
		else
		{
			Logger::Get()->Log(samplog_LogLevel::ERROR,
				"Can't read from Discord websocket gateway: {} ({})",
				ec.message(),
				ec.value());
			reconnect = true;
		}

		if (reconnect)
		{
			Logger::Get()->Log(samplog_LogLevel::INFO,
				"websocket gateway connection terminated; attempting reconnect...");
			Disconnect(true);
		}
		else if (ec == beast::websocket::error::closed
			|| ec == asio::ssl::error::stream_errors::stream_truncated)
		{
			Disconnect(false);
		}
		return;
	}

	json result;
	try
	{
		result = json::parse(beast::buffers_to_string(_buffer.data()));
	}
	catch (json::exception const &e)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR,
			"invalid Discord gateway payload: {}", e.what());
		_buffer.clear();
		Disconnect(true);
		return;
	}
	_buffer.clear();

	int payload_opcode = result["op"].get<int>();
	switch (payload_opcode)
	{
	case 0:
	{
		_sequenceNumber = result["s"];
		m_HasSequenceNumber = true;

#define __WS_EVENT_MAP_PAIR(event) { #event, Event::event }
		static const std::unordered_map<std::string, Event> events_map{
			__WS_EVENT_MAP_PAIR(READY),
			__WS_EVENT_MAP_PAIR(RESUMED),
			__WS_EVENT_MAP_PAIR(GUILD_CREATE),
			__WS_EVENT_MAP_PAIR(GUILD_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_DELETE),
			__WS_EVENT_MAP_PAIR(GUILD_ROLE_CREATE),
			__WS_EVENT_MAP_PAIR(GUILD_ROLE_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_ROLE_DELETE),
			__WS_EVENT_MAP_PAIR(CHANNEL_CREATE),
			__WS_EVENT_MAP_PAIR(CHANNEL_UPDATE),
			__WS_EVENT_MAP_PAIR(CHANNEL_DELETE),
			__WS_EVENT_MAP_PAIR(CHANNEL_PINS_UPDATE),
			__WS_EVENT_MAP_PAIR(THREAD_CREATE),
			__WS_EVENT_MAP_PAIR(THREAD_UPDATE),
			__WS_EVENT_MAP_PAIR(THREAD_DELETE),
			__WS_EVENT_MAP_PAIR(THREAD_LIST_SYNC),
			__WS_EVENT_MAP_PAIR(THREAD_MEMBER_UPDATE),
			__WS_EVENT_MAP_PAIR(THREAD_MEMBERS_UPDATE),
			__WS_EVENT_MAP_PAIR(STAGE_INSTANCE_CREATE),
			__WS_EVENT_MAP_PAIR(STAGE_INSTANCE_UPDATE),
			__WS_EVENT_MAP_PAIR(STAGE_INSTANCE_DELETE),
			__WS_EVENT_MAP_PAIR(GUILD_MEMBER_ADD),
			__WS_EVENT_MAP_PAIR(GUILD_MEMBER_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_MEMBER_REMOVE),
			__WS_EVENT_MAP_PAIR(THREAD_MEMBERS_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_AUDIT_LOG_ENTRY_CREATE),
			__WS_EVENT_MAP_PAIR(GUILD_BAN_ADD),
			__WS_EVENT_MAP_PAIR(GUILD_BAN_REMOVE),
			__WS_EVENT_MAP_PAIR(GUILD_EMOJIS_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_STICKERS_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_INTEGRATIONS_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_MEMBERS_CHUNK),
			__WS_EVENT_MAP_PAIR(INTEGRATION_CREATE),
			__WS_EVENT_MAP_PAIR(INTEGRATION_UPDATE),
			__WS_EVENT_MAP_PAIR(INTEGRATION_DELETE),
			__WS_EVENT_MAP_PAIR(WEBHOOKS_UPDATE),
			__WS_EVENT_MAP_PAIR(INVITE_CREATE),
			__WS_EVENT_MAP_PAIR(INVITE_DELETE),
			__WS_EVENT_MAP_PAIR(VOICE_STATE_UPDATE),
			__WS_EVENT_MAP_PAIR(PRESENCE_UPDATE),
			__WS_EVENT_MAP_PAIR(MESSAGE_CREATE),
			__WS_EVENT_MAP_PAIR(MESSAGE_UPDATE),
			__WS_EVENT_MAP_PAIR(MESSAGE_DELETE),
			__WS_EVENT_MAP_PAIR(MESSAGE_DELETE_BULK),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_ADD),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_REMOVE),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_REMOVE_ALL),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_REMOVE_EMOJI),
			__WS_EVENT_MAP_PAIR(TYPING_START),
			__WS_EVENT_MAP_PAIR(MESSAGE_CREATE),
			__WS_EVENT_MAP_PAIR(MESSAGE_UPDATE),
			__WS_EVENT_MAP_PAIR(MESSAGE_DELETE),
			__WS_EVENT_MAP_PAIR(CHANNEL_PINS_UPDATE),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_ADD),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_REMOVE),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_REMOVE_ALL),
			__WS_EVENT_MAP_PAIR(MESSAGE_REACTION_REMOVE_EMOJI),
			__WS_EVENT_MAP_PAIR(TYPING_START),
			//__WS_EVENT_MAP_PAIR(MESSAGE_CONTENT),
			__WS_EVENT_MAP_PAIR(GUILD_SCHEDULED_EVENT_CREATE),
			__WS_EVENT_MAP_PAIR(GUILD_SCHEDULED_EVENT_UPDATE),
			__WS_EVENT_MAP_PAIR(GUILD_SCHEDULED_EVENT_DELETE),
			__WS_EVENT_MAP_PAIR(GUILD_SCHEDULED_EVENT_USER_ADD),
			__WS_EVENT_MAP_PAIR(GUILD_SCHEDULED_EVENT_USER_REMOVE),
			__WS_EVENT_MAP_PAIR(AUTO_MODERATION_RULE_CREATE),
			__WS_EVENT_MAP_PAIR(AUTO_MODERATION_RULE_UPDATE),
			__WS_EVENT_MAP_PAIR(AUTO_MODERATION_RULE_DELETE),
			__WS_EVENT_MAP_PAIR(AUTO_MODERATION_ACTION_EXECUTION),
			__WS_EVENT_MAP_PAIR(INTERACTION_CREATE),
		};

		auto it = events_map.find(result["t"].get<std::string>());
		if (it != events_map.end())
		{
			json &data = result["d"];
			Event event = it->second;

			if (event == Event::READY)
			{
				m_SessionId = data["session_id"].get<std::string>();
				if (data.find("resume_gateway_url") != data.end())
				{
					m_ResumeGatewayUrl = data["resume_gateway_url"].get<std::string>();
					auto protocol = m_ResumeGatewayUrl.find("wss://");
					if (protocol == 0)
						m_ResumeGatewayUrl.erase(0, 6);
					auto slash = m_ResumeGatewayUrl.find('/');
					if (slash != std::string::npos)
						m_ResumeGatewayUrl.erase(slash);
				}
				m_ShouldResume = true;
				m_ConnectionState = ConnectionState::READY;
				_reconnectCount = 0;
				FlushPresence();
			}
			else if (event == Event::RESUMED)
			{
				m_ShouldResume = true;
				m_ConnectionState = ConnectionState::READY;
				_reconnectCount = 0;
				FlushPresence();
			}

			auto event_range = m_EventMap.equal_range(event);
			for (auto ev_it = event_range.first; ev_it != event_range.second; ++ev_it)
				ev_it->second(data);
		}
		else
		{
			Logger::Get()->Log(samplog_LogLevel::WARNING, "Unknown gateway event '{}'", result["t"].get<std::string>());
			Logger::Get()->Log(samplog_LogLevel::DEBUG, "UGE res: {}", result.dump(4));
		}
	} break;
	case 7: // reconnect
		Logger::Get()->Log(samplog_LogLevel::INFO,
			"websocket gateway requested reconnect; attempting reconnect...");
		Disconnect(true);
		return;
	case 9: // invalid session
		m_ShouldResume = result["d"].is_boolean() && result["d"].get<bool>()
			&& !m_SessionId.empty() && m_HasSequenceNumber;
		if (!m_ShouldResume)
			ResetSession();
		Disconnect(true);
		return;
	case 10: // hello
		// at this point we're connected to the gateway, but not authenticated
		// start heartbeat
		m_HeartbeatInterval = std::chrono::milliseconds(result["d"]["heartbeat_interval"]);
		m_AwaitingHeartbeatAck = false;
		{
			std::random_device rd;
			std::uniform_real_distribution<double> jitter(0.0, 1.0);
			m_HeartbeatTimer.expires_after(
				std::chrono::duration_cast<std::chrono::steady_clock::duration>(
					m_HeartbeatInterval * jitter(rd)));
			m_HeartbeatTimer.async_wait(
				beast::bind_front_handler(&WebSocket::DoHeartbeat, this));
		}
		m_ConnectionState = ConnectionState::AUTHENTICATING;
		if (m_ShouldResume && !m_SessionId.empty() && m_HasSequenceNumber)
			SendResumePayload();
		else
			Identify();
		break;
	case 11: // heartbeat ACK
		m_AwaitingHeartbeatAck = false;
		Logger::Get()->Log(samplog_LogLevel::DEBUG, "heartbeat ACK");
		break;
	case 1: // Discord requested an immediate heartbeat
		SendHeartbeat();
		break;
	default:
		Logger::Get()->Log(samplog_LogLevel::WARNING, "Unhandled payload opcode '{}'", payload_opcode);
		Logger::Get()->Log(samplog_LogLevel::DEBUG, "UPO res: {}", result.dump(4));
	}

	Read();
}

void WebSocket::Write(std::string data, bool require_authenticated /*= false*/)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::Write");

	asio::post(_ioContext, [this, data = std::move(data), require_authenticated]() mutable
	{
		if (!_websocket || !_websocket->is_open() || m_CloseRequested)
			return;
		if (require_authenticated && m_ConnectionState != ConnectionState::READY)
			return;

		bool idle = m_WriteQueue.empty();
		m_WriteQueue.emplace_back(std::move(data));
		if (idle)
			DoWrite();
	});
}

void WebSocket::DoWrite()
{
	if (m_WriteQueue.empty() || !_websocket || !_websocket->is_open())
		return;

	_websocket->async_write(
		asio::buffer(m_WriteQueue.front()),
		beast::bind_front_handler(&WebSocket::OnWrite, this));
}

void WebSocket::OnWrite(beast::error_code ec,
	size_t bytes_transferred)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, 
		"WebSocket::OnWrite({:d})", 
		bytes_transferred);

	if (ec)
	{
		Logger::Get()->Log(samplog_LogLevel::ERROR,
			"Can't write to Discord websocket gateway: {} ({})",
			ec.message(), ec.value());

		m_WriteQueue.clear();
		Disconnect(true);
		return;
	}

	if (!m_WriteQueue.empty())
		m_WriteQueue.pop_front();
	if (!m_WriteQueue.empty())
		DoWrite();
	else if (m_CloseRequested)
		BeginClose();
}

void WebSocket::Identify()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::Identify");

	const char *os_name =
#ifdef WIN32
		"Windows";
#else
		"Linux";
#endif

	json identify_payload = {
		{ "op", 2 },
		{ "d",{
			{ "token", _apiToken },
			{ "compress", false },
			{ "intents", _intents },
			{ "large_threshold", LARGE_THRESHOLD_NUMBER },
			{ "properties",{
				{ "$os", os_name },
				{ "$browser", BOOST_BEAST_VERSION_STRING },
				{ "$device", "SA-MP/open.mp DCC plugin" },
				{ "$referrer", "" },
				{ "$referring_domain", "" }
			} }
		} }
	};

	Write(identify_payload.dump());
}

void WebSocket::SendResumePayload()
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::SendResumePayload");

	json resume_payload = {
		{ "op", 6 },
		{ "d",{
			{ "token", _apiToken },
			{ "session_id", m_SessionId },
			{ "seq", _sequenceNumber }
		} }
	};

	Write(resume_payload.dump());
}

void WebSocket::RequestGuildMembers(std::string guild_id)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::RequestGuildMembers");

	json payload = {
		{ "op", 8 },
		{ "d",{
			{ "guild_id", guild_id },
			{ "query", "" },
			{ "limit", 0 }
		} }
	};

	Write(payload.dump(), true);
}

void WebSocket::UpdateStatus(std::string const &status, std::string const &activity_name)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::UpdateStatus");
	asio::post(_ioContext, [this, status, activity_name]()
	{
		m_PendingPresenceStatus = status;
		m_PendingActivityName = activity_name;
		m_PresenceUpdatePending = true;
		FlushPresence();
	});
}

void WebSocket::FlushPresence(beast::error_code ec /*= {}*/)
{
	if (ec == asio::error::operation_aborted)
		return;
	m_PresenceTimerScheduled = false;

	if (!m_PresenceUpdatePending || m_ConnectionState != ConnectionState::READY)
		return;

	auto now = std::chrono::steady_clock::now();
	auto minimum_interval = std::chrono::seconds(15);
	if (m_LastPresenceUpdate.time_since_epoch().count() != 0
		&& now - m_LastPresenceUpdate < minimum_interval)
	{
		if (!m_PresenceTimerScheduled)
		{
			m_PresenceTimerScheduled = true;
			m_PresenceTimer.expires_at(m_LastPresenceUpdate + minimum_interval);
			m_PresenceTimer.async_wait(
				beast::bind_front_handler(&WebSocket::FlushPresence, this));
		}
		return;
	}

	json payload = {
		{ "op", 3 },
		{ "d", {
			{ "since", nullptr },
			{ "activities", json::array() },
			{ "status", m_PendingPresenceStatus },
			{ "afk", false },
		} }
	};

	if (!m_PendingActivityName.empty())
	{
		payload.at("d").at("activities").push_back({
			{ "name", m_PendingActivityName },
			{ "type", 0 }
		});
	}

	m_PresenceUpdatePending = false;
	m_LastPresenceUpdate = now;
	Write(payload.dump(), true);
}

void WebSocket::DoHeartbeat(beast::error_code ec)
{
	Logger::Get()->Log(samplog_LogLevel::DEBUG, "WebSocket::DoHeartbeat");

	if (ec)
	{
		switch (ec.value())
		{
		case boost::asio::error::operation_aborted:
			// timer was chancelled, do nothing
			Logger::Get()->Log(samplog_LogLevel::DEBUG, "heartbeat timer chancelled");
			break;
		default:
			Logger::Get()->Log(samplog_LogLevel::ERROR, "Heartbeat error: {} ({})",
				ec.message(), ec.value());
			break;
		}
		return;
	}

	if (m_AwaitingHeartbeatAck)
	{
		Logger::Get()->Log(samplog_LogLevel::WARNING,
			"heartbeat ACK was not received; reconnecting gateway");
		Disconnect(true);
		return;
	}

	SendHeartbeat();

	m_HeartbeatTimer.expires_after(m_HeartbeatInterval);
	m_HeartbeatTimer.async_wait(
		beast::bind_front_handler(
			&WebSocket::DoHeartbeat,
			this));
}

void WebSocket::SendHeartbeat()
{
	json heartbeat_payload = {
		{ "op", 1 },
		{ "d", m_HasSequenceNumber ? json(_sequenceNumber) : json(nullptr) }
	};

	Logger::Get()->Log(samplog_LogLevel::DEBUG, "sending heartbeat");
	m_AwaitingHeartbeatAck = true;
	Write(heartbeat_payload.dump());
}

void WebSocket::ResetSession()
{
	m_ShouldResume = false;
	m_SessionId.clear();
	m_ResumeGatewayUrl.clear();
	_sequenceNumber = 0;
	m_HasSequenceNumber = false;
}
