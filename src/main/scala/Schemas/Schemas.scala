package Schemas

object Schemas {
    case class Tweet(userId: Int, source: String)
    case class TweetConsumerMessage(data: String)
}
