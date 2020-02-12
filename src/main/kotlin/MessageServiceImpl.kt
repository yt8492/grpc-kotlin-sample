import com.yt8492.grpckotlinsample.protobuf.MessageRequest
import com.yt8492.grpckotlinsample.protobuf.MessageResponse
import com.yt8492.grpckotlinsample.protobuf.MessageServiceImplBase
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.toList

@ExperimentalCoroutinesApi
class MessageServiceImpl : MessageServiceImplBase() {

    override suspend fun unary(request: MessageRequest): MessageResponse {
        val response = MessageResponse.newBuilder()
            .setMessage(request.message.toUpperCase())
            .build()
        return response
    }

    override suspend fun clientStream(
        requests: ReceiveChannel<MessageRequest>
    ): MessageResponse {
        val requestList = requests.toList()
        val response = MessageResponse.newBuilder()
            .setMessage(
                requestList.joinToString("\n") {
                    it.message.toUpperCase()
                }
            )
            .build()
        return response
    }

    override fun serverStream(
        request: MessageRequest
    ): ReceiveChannel<MessageResponse> {
        val response = MessageResponse.newBuilder()
            .setMessage(request.message.toUpperCase())
            .build()
        return produce {
            repeat(2) {
                send(response)
            }
        }
    }

    override fun bidirectionalStream(
        requests: ReceiveChannel<MessageRequest>
    ): ReceiveChannel<MessageResponse> {
        return produce {
            requests.consumeEach { request ->
                val response = MessageResponse.newBuilder()
                    .setMessage(request.message.toUpperCase())
                    .build()
                send(response)
            }
        }
    }
}