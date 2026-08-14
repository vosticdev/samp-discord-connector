#pragma once

#include <string>
#include <functional>
#include <chrono>
#include <map>
#include <thread>
#include <memory>
#include <deque>
#include <atomic>

#include <json.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

using json = nlohmann::json;
namespace asio = boost::asio;
namespace beast = boost::beast;

class WebSocket
{
	friend class Network;
public:
	enum class Event
	{
		READY,
		RESUMED,
		CHANNEL_CREATE,
		CHANNEL_UPDATE,
		CHANNEL_DELETE,
		CHANNEL_PINS_UPDATE,
		GUILD_CREATE,
		GUILD_UPDATE,
		GUILD_DELETE,
		GUILD_BAN_ADD,
		GUILD_BAN_REMOVE,
		GUILD_EMOJIS_UPDATE,
		GUILD_INTEGRATIONS_UPDATE,
		GUILD_MEMBER_ADD,
		GUILD_MEMBER_REMOVE,
		GUILD_MEMBER_UPDATE,
		GUILD_MEMBERS_CHUNK,
		GUILD_ROLE_CREATE,
		GUILD_ROLE_UPDATE,
		GUILD_ROLE_DELETE,
		MESSAGE_CREATE,
		MESSAGE_UPDATE,
		MESSAGE_DELETE,
		MESSAGE_DELETE_BULK,
		MESSAGE_REACTION_ADD,
		MESSAGE_REACTION_REMOVE,
		MESSAGE_REACTION_REMOVE_ALL,
		MESSAGE_REACTION_REMOVE_EMOJI,
		PRESENCE_UPDATE,
		TYPING_START,
		USER_UPDATE,
		VOICE_STATE_UPDATE,
		VOICE_SERVER_UPDATE,
		WEBHOOKS_UPDATE,
		PRESENCES_REPLACE,
		INVITE_CREATE,
		INVITE_DELETE,
		INTERACTION_CREATE,
		THREAD_CREATE,
		THREAD_UPDATE,
		THREAD_DELETE,
		THREAD_LIST_SYNC,
		THREAD_MEMBER_UPDATE,
		THREAD_MEMBERS_UPDATE,
		STAGE_INSTANCE_CREATE,
		STAGE_INSTANCE_UPDATE,
		STAGE_INSTANCE_REMOVE,
		STAGE_INSTANCE_DELETE,
		GUILD_AUDIT_LOG_ENTRY_CREATE,
		GUILD_STICKERS_UPDATE,
		INTEGRATION_CREATE,
		INTEGRATION_UPDATE,
		INTEGRATION_DELETE,
		MESSAGE_CONTENT,
		GUILD_SCHEDULED_EVENT_CREATE,
		GUILD_SCHEDULED_EVENT_UPDATE,
		GUILD_SCHEDULED_EVENT_DELETE,
		GUILD_SCHEDULED_EVENT_USER_ADD,
		GUILD_SCHEDULED_EVENT_USER_REMOVE,
		AUTO_MODERATION_RULE_CREATE,
		AUTO_MODERATION_RULE_UPDATE,
		AUTO_MODERATION_RULE_DELETE,
		AUTO_MODERATION_ACTION_EXECUTION,
	};
	using EventCallback_t = std::function<void(json const &)>;

private:
	WebSocket();

public:
	~WebSocket();

private: // variables
	enum class ConnectionState
	{
		DISCONNECTED,
		CONNECTING,
		AWAITING_HELLO,
		AUTHENTICATING,
		READY
	};

	const int LARGE_THRESHOLD_NUMBER = 100;

	asio::io_context _ioContext;
	std::unique_ptr<std::thread> _netThread;
	asio::ip::tcp::resolver _resolver;
	asio::ssl::context _sslContext;
	using SslStream_t = beast::ssl_stream<beast::tcp_stream>;
	using WebSocketStream_t = beast::websocket::stream<SslStream_t>;
	std::unique_ptr<WebSocketStream_t> _websocket;

	asio::steady_timer _reconnectTimer;
	unsigned int _reconnectCount = 0;
	ConnectionState m_ConnectionState = ConnectionState::DISCONNECTED;
	bool m_ShouldResume = false;
	bool m_ReconnectScheduled = false;
	bool m_CloseRequested = false;
	bool m_CloseInProgress = false;
	bool m_ReconnectAfterClose = false;
	std::atomic<bool> m_ShuttingDown{ false };
	std::deque<std::string> m_WriteQueue;

	beast::multi_buffer _buffer;

	std::string _apiToken;
	std::string _gatewayUrl;
	std::string m_ConnectingGatewayUrl;
	uint64_t _sequenceNumber = 0;
	bool m_HasSequenceNumber = false;
	std::string m_SessionId;
	std::string m_ResumeGatewayUrl;
	asio::steady_timer m_HeartbeatTimer;
	std::chrono::steady_clock::duration m_HeartbeatInterval;
	bool m_AwaitingHeartbeatAck = false;
	asio::steady_timer m_PresenceTimer;
	std::chrono::steady_clock::time_point m_LastPresenceUpdate;
	std::string m_PendingPresenceStatus;
	std::string m_PendingActivityName;
	bool m_PresenceUpdatePending = false;
	bool m_PresenceTimerScheduled = false;
	std::multimap<Event, EventCallback_t> m_EventMap;
	int _intents;

private: // functions
	void Initialize(std::string token, std::string gateway_url, int intents);

	void Connect();
	void OnResolve(beast::error_code ec,
		asio::ip::tcp::resolver::results_type results);
	void OnConnect(beast::error_code ec,
		asio::ip::tcp::resolver::results_type::endpoint_type ep);
	void OnSslHandshake(beast::error_code ec);
	void OnHandshake(beast::error_code ec);

	void Disconnect(bool reconnect = false);
	void BeginClose();
	void ScheduleReconnect();
	void OnClose(beast::error_code ec);
	void OnReconnect(beast::error_code ec);

	void Read();
	void OnRead(beast::error_code ec,
		std::size_t bytes_transferred);

	void Write(std::string data, bool require_authenticated = false);
	void DoWrite();
	void OnWrite(beast::error_code ec,
		size_t bytes_transferred);

	void Identify();
	void SendResumePayload();
	void DoHeartbeat(beast::error_code ec);
	void SendHeartbeat();
	void ResetSession();
	void FlushPresence(beast::error_code ec = {});

public: // functions
	void RegisterEvent(Event event, EventCallback_t &&callback)
	{
		m_EventMap.emplace(event, std::move(callback));
	}
	void RequestGuildMembers(std::string guild_id);
	void UpdateStatus(std::string const &status, std::string const &activity_name);
};
