## Using Reader and IO Monads with Spark

This is a simple example using Reader and IO Monads to facilitate DI and pushing side effects to the boundaries, it also includes some basic BDD tests with FlatSpec suite

- [Dataset](http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html)

### Part A

Create a list of user IDs, along with the number of distinct songs each user has played.

### Part B

Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of

times

each was played.

### Part C

Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played

by that user, where each song is started within 20 minutes of the previous song’s start time.

Create a list of the top 10 longest sessions, with the following information about each session:

userid, timestamp of first and last songs in the session, and the list of songs played in the

session (in order of play).