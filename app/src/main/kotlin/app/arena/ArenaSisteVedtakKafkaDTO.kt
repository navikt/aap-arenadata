package app.arena

data class FinnesVedtakKafkaDTO(
    val req: Request,
    val res: Response?
)

data class Request(
    val sjekkKelvin: Boolean,
    val sjekkArena: Boolean
)

data class Response(
    val finnesIKelvin: Boolean?,
    val finnesIArena: Boolean?
)