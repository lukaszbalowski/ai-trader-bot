import asyncio
import aiohttp
import json

async def main():
    # Searching for active events containing the phrase "Bitcoin Up or Down"
    url = "https://gamma-api.polymarket.com/events?search=Bitcoin%20Up%20or%20Down&active=true&closed=false&limit=10"
    
    print(f"📡 Querying Gamma API for market lists...")
    print(f"URL: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    filename = "data/markets_list_dump.json"
                    
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)
                        
                    print(f"✅ SUCCESS! Raw API response saved to file: {filename}")
                    print("Open the file and find the market corresponding to the current hour.")
                    print("Look for the 'Price to beat' (Strike Price) value and find its corresponding key!")
                else:
                    print(f"❌ Polymarket Server Error: Status {resp.status}")
    except Exception as e:
        print(f"❌ Connection Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())