module Poseidon
  # High level internal interface to a remote broker. Provides access to
  # the broker API.
  # @api private
  class Connection
    include Protocol

    class ConnectionFailedError < StandardError; end
    class TimeoutException < Exception; end

    API_VERSION = 0
    REPLICA_ID = -1 # Replica id is always -1 for non-brokers

    # @yieldparam [Connection]
    def self.open(host, port, client_id, socket_timeout_ms, &block)
      connection = new(host, port, client_id, socket_timeout_ms)

      yield connection
    ensure
      connection.close
    end

    attr_reader :host, :port

    # Create a new connection
    #
    # @param [String] host Host to connect to
    # @param [Integer] port Port broker listens on
    # @param [String] client_id Unique across processes?
    def initialize(host, port, client_id, socket_timeout_ms)
      @host = host
      @port = port

      @client_id = client_id
      @socket_timeout_ms = socket_timeout_ms
    end

    # Close broker connection
    def close
      @socket && @socket.close
    end

    # Execute a produce call
    #
    # @param [Integer] required_acks
    # @param [Integer] timeout
    # @param [Array<Protocol::MessagesForTopics>] messages_for_topics Messages to send
    # @return [ProduceResponse]
    def produce(required_acks, timeout, messages_for_topics)
      ensure_connected
      req = ProduceRequest.new( request_common(:produce),
                                required_acks,
                                timeout,
                                messages_for_topics)
      send_request(req)
      if required_acks != 0
        read_response(ProduceResponse)
      else
        true
      end
    end

    # Execute a fetch call
    #
    # @param [Integer] max_wait_time
    # @param [Integer] min_bytes
    # @param [Integer] topic_fetches
    def fetch(max_wait_time, min_bytes, topic_fetches)
      ensure_connected
      req = FetchRequest.new( request_common(:fetch),
                                REPLICA_ID,
                                max_wait_time,
                                min_bytes,
                                topic_fetches)
      send_request(req)
      read_response(FetchResponse)
    end

    def offset(offset_topic_requests)
      ensure_connected
      req = OffsetRequest.new(request_common(:offset),
                              REPLICA_ID,
                              offset_topic_requests)
      send_request(req)
      read_response(OffsetResponse).topic_offset_responses
    end

    # Fetch metadata for +topic_names+
    #
    # @param [Enumberable<String>] topic_names
    #   A list of topics to retrive metadata for
    # @return [TopicMetadataResponse] metadata for the topics
    def topic_metadata(topic_names)
      ensure_connected
      req = MetadataRequest.new( request_common(:metadata),
                                 topic_names)
      send_request(req)
      read_response(MetadataResponse)
    end

    private
    def ensure_connected
      if @socket.nil? || @socket.closed?
        begin
          @socket = connect_with_timeout(@host, @port, @socket_timeout_ms / 1000.0)
        rescue SystemCallError
          raise_connection_failed_error
        end
      end
    end

    def connect_with_timeout(host, port, timeout = 5)
      # Convert the passed host into structures the non-blocking calls can deal with
      addr = Socket.getaddrinfo(host, nil)
      sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

      Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0).tap do |socket|
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

        begin
          # Initiate the socket connection in the background. If it doesn't fail
          # immediatelyit will raise an IO::WaitWritable (Errno::EINPROGRESS)
          # indicating the connection is in progress.
          socket.connect_nonblock(sockaddr)

        rescue IO::WaitWritable
          # IO.select will block until the socket is writable or the timeout
          # is exceeded - whichever comes first.
          if IO.select(nil, [socket], nil, timeout)
            begin
              # Verify there is now a good connection
              socket.connect_nonblock(sockaddr)
            rescue Errno::EISCONN
              # Good news everybody, the socket is connected!
            rescue
              # An unexpected exception was raised - the connection is no good.
              socket.close
              raise
            end
          else
            # IO.select returns nil when the socket is not ready before timeout
            # seconds have elapsed
            socket.close
            raise "Connection timeout"
          end
        end
      end
    end

    def read_response(response_class)
      r = ensure_read_or_timeout(4)
      if r.nil?
        raise_connection_failed_error
      end
      n = r.unpack("N").first
      s = ensure_read_or_timeout(n)
      buffer = Protocol::ResponseBuffer.new(s)
      response_class.read(buffer)
    rescue Errno::ECONNRESET, SocketError, Errno::ETIMEDOUT, TimeoutException
      @socket = nil
      raise_connection_failed_error
    end

    def ensure_read_or_timeout(maxlen)
      if IO.select([@socket], nil, nil, @socket_timeout_ms / 1000.0)
        # There is a potential race condition here, where the socket could time out between IO Select and Write
        # This will throw a Errno::ETIMEDOUT, but it will be caught in the calling code
         @socket.read(maxlen)
      else
         raise TimeoutException.new
      end
    end

    def send_request(request)
      buffer = Protocol::RequestBuffer.new
      request.write(buffer)
      ensure_write_or_timeout([buffer.to_s.bytesize].pack("N") + buffer.to_s)
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT, TimeoutException
      @socket = nil
      raise_connection_failed_error
    end

    def ensure_write_or_timeout(data)
      if IO.select(nil, [@socket], nil, @socket_timeout_ms / 1000.0)
        # There is a potential race condition here, where the socket could time out between IO Select and Write
        # This will throw a Errno::ETIMEDOUT, but it will be caught in the calling code
        @socket.write(data)
      else
        raise TimeoutException.new
      end
    end

    def request_common(request_type)
      RequestCommon.new(
        API_KEYS[request_type],
        API_VERSION,
        next_correlation_id,
        @client_id
      )
    end

    def next_correlation_id
      @correlation_id ||= 0
      @correlation_id  += 1
    end

    def raise_connection_failed_error
      raise ConnectionFailedError, "Failed to connect to #{@host}:#{@port}"
    end
  end
end
