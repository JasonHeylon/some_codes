require 'sneakers/handlers/maxretry'
require 'base64'
require 'json'

module Sneakers
  module Handlers
    class MyMaxretry < Maxretry
      # Helper logic for retry handling. This will reject the message if there
      # are remaining retries left on it, otherwise it will publish it to the
      # error exchange along with the reason.
      # @param hdr [Bunny::DeliveryInfo]
      # @param props [Bunny::MessageProperties]
      # @param msg [String] The message
      # @param reason [String, Symbol, Exception] Reason for the retry, included
      #   in the JSON we put on the error exchange.
      def handle_retry(hdr, props, msg, reason)
        # +1 for the current attempt
        num_attempts = failure_count(props[:headers]) + 1
        if num_attempts <= @max_retries
          # We call reject which will route the message to the
          # x-dead-letter-exchange (ie. retry exchange) on the queue
          Sneakers.logger.info do
            "#{log_prefix} msg=retrying, count=#{num_attempts}, headers=#{props[:headers]}"
          end
          @channel.reject(hdr.delivery_tag, false)
          # TODO: metrics
        else
          # Retried more than the max times
          # Publish the original message with the routing_key to the error exchange
          Sneakers.logger.info do
            "#{log_prefix} msg=failing, retry_count=#{num_attempts}, reason=#{reason}"
          end
          data = {
            error: reason,
            num_attempts: num_attempts,
            failed_at: Time.now.iso8601,
            # original payload
            payload: Base64.encode64(msg.to_s),
            #original queue
            queue_name: @worker_queue_name,
            # original routing_key
            routing_key: hdr.routing_key,
            # routing exchange
            exchange_name: hdr.exchange
          }.tap do |hash|
            if reason.is_a?(Exception)
              hash[:error_class] = reason.class
              hash[:error_message] = "#{reason}"
              if reason.backtrace
                hash[:backtrace] = reason.backtrace.take(10).join(', ')
              end
            end
          end.to_json
          @error_exchange.publish(data, :routing_key => hdr.routing_key)
          @channel.acknowledge(hdr.delivery_tag, false)
          # TODO: metrics
        end
      end
      private :handle_retry

    end
  end
end
