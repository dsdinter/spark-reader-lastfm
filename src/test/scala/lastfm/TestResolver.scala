package lastfm

import org.scalatest.{FlatSpec, Matchers}

import Resolver._

import scalaz.Reader

/**
  * Created by davidsabater on 14/03/2017.
  */
class TestResolver extends FlatSpec with Matchers with Context {

  "resolvePartA" should "return a list of user IDs, along with the number of distinct songs each user has played" in {
    runWithSpark(Reader(sc => {
      val listenedSongs = sc.parallelize(List(
        ListenedSongs("User1", 1229827261000L, Song("Track1", "ArtistA")),
        ListenedSongs("User1", 1229827263000L, Song("Track2", "ArtistA")),
        ListenedSongs("User2", 1229827265000L, Song("Track3", "ArtistA")),
        ListenedSongs("User2", 1229827267000L, Song("Track3", "ArtistA"))
      ))
      val result = resolvePartA(listenedSongs)
      result should be(Map(
        "User1" -> 2L,
        "User2" -> 1L
      ))
    }))
  }

  "resolvePartB" should "return a list of the 100 most popular songs (artist and title) in the dataset, with the number of " +
    "times each was played." in {
    runWithSpark(Reader(sc => {
      val listenedSongs = sc.parallelize(List(
        ListenedSongs("User1", 1229827261000L, Song("Track1", "ArtistA")),
        ListenedSongs("User1", 1229827263000L, Song("Track2", "ArtistA")),
        ListenedSongs("User1", 1229827263000L, Song("Track2", "ArtistA")),
        ListenedSongs("User2", 1229827265000L, Song("Track3", "ArtistA")),
        ListenedSongs("User2", 1229827265000L, Song("Track3", "ArtistA")),
        ListenedSongs("User2", 1229827267000L, Song("Track3", "ArtistA"))
      ))
      val result = resolvePartB(listenedSongs)
      result should be(Map(
        Song("Track3", "ArtistA") -> 3L,
        Song("Track2", "ArtistA") -> 2L,
        Song("Track1", "ArtistA") -> 1L
      ))
    }))
  }

  "resolvePartC" should "return a list of the top 10 longest sessions, with the following information about each session:" +
    "userid, timestamp of first and last songs in the session, and the list of songs played in the " +
    "session (in order of play)." in {
    runWithSpark(Reader(sc => {
      val listenedSongs = sc.parallelize(List(
        ListenedSongs("User1", 1229827261000L, Song("Track1", "ArtistA")),
        ListenedSongs("User1", 1229827663000L, Song("Track2", "ArtistA")),
        ListenedSongs("User1", 1229827461000L, Song("Track5", "ArtistB")),
        ListenedSongs("User2", 1229827265000L, Song("Track3", "ArtistA")),
        ListenedSongs("User2", 1229827467000L, Song("Track4", "ArtistA"))
      ))
      val result = resolvePartC(listenedSongs, 10)
      result should be(Map(
        "User1" -> Session(1229827261000L, 1229827663000L, List(
          (Song("Track1", "ArtistA"), 1229827261000L),
          (Song("Track5", "ArtistB"), 1229827461000L),
          (Song("Track2", "ArtistA"), 1229827663000L)
        )),
        "User2" -> Session(1229827265000L, 1229827467000L, List(
          (Song("Track3", "ArtistA"), 1229827265000L),
          (Song("Track4", "ArtistA"), 1229827467000L)
        ))
      ))
    }))
  }

}
