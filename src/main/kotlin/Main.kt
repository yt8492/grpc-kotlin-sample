import io.grpc.ServerBuilder
import kotlinx.coroutines.ExperimentalCoroutinesApi

@ExperimentalCoroutinesApi
fun main() {
    val port = 6565
    val server = ServerBuilder.forPort(port)
        .addService(MessageServiceImpl())
        .build()
        .start()
    Runtime.getRuntime().addShutdownHook(Thread() {
        server.shutdown()
    })
    server.awaitTermination()
}