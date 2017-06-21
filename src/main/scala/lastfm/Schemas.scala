package lastfm

case class ListenedSongs(userID: String, timestamp: Long, song: Song)
case class Song(trackName: String, artistName: String)
case class Session(startTime: Long, stopTime: Long, songs: List[(Song, Long)])