import psycopg2

try:
    conn = psycopg2.connect("dbname=udacity user=udacity password=udacity")
except Exception as e:
    print(e)

try:
    cur = conn.cursor()
except Exception as e:
    print(e)

conn.set_session(autocommit=True)

try:
    cur.execute("DROP TABLE album_legnth")
except Exception as e:
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS album_length (song_id int, album_name varchar, \
                song_length int);")
except Exception as e:
    print(e)

songs = []

cur.execute("SELECT song_id, song_length, album_id FROM song_library1")

songs_row = cur.fetchone()
while songs_row:
    songs.append(songs_row)
    songs_row = cur.fetchone()

albums = {}

cur.execute("SELECT album_id, album_name FROM album_library1")

albums_row = cur.fetchone()
while albums_row:
    albums[albums_row[0]] = albums_row[1]
    albums_row = cur.fetchone()


for song in songs:
    cur.execute("INSERT INTO album_length " \
                 "VALUES(%s, %s, %s)" \
                ,(song[0], albums.get(song[2]), (song[1])))
