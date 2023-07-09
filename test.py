import asyncio
from spotipy2 import Spotify
from spotipy2.auth import ClientCredentialsFlow


# Print an artist's popularity score
async def print_popularity_score(artist_id):
    # Authenticate using ClientCredentialsFlow
    spo_client = Spotify(
        ClientCredentialsFlow(
            client_id="a7a682c824314d94a5ee25a389678410",
            client_secret="10e314f4fe8c42af8588572e6746c05b",
            token=None,
        )
    )

    # Use the Spotify client to get artist's info
    async with spo_client as s:
        artist = await s.get_artist(artist_id)
        print(f"{artist.name}'s popularity score is {artist.popularity}/100")

# Print popularity score of Ed Sheeran
asyncio.run(print_popularity_score("6eUKZXaKkcviH0Ku9w2n3V"))
