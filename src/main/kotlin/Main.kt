import io.grpc.ServerBuilder

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