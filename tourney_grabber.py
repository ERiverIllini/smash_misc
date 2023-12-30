import requests  
import json
import pandas as pd
import os
from datetime import datetime

request_url = 'https://api.start.gg/gql/alpha' 

# Use your token here
AUTH_TOKEN = ""
# Stole this from https://stackoverflow.com/questions/39899005/how-to-flatten-a-pandas-dataframe-with-some-columns-as-json
def flatten_nested_json_df(df):
    
    df = df.reset_index()
    
    print(f"original shape: {df.shape}")
    print(f"original columns: {df.columns}")
    
    
    # search for columns to explode/flatten
    s = (df.applymap(type) == list).all()
    list_columns = s[s].index.tolist()
    
    s = (df.applymap(type) == dict).all()
    dict_columns = s[s].index.tolist()
    
    print(f"lists: {list_columns}, dicts: {dict_columns}")
    while len(list_columns) > 0 or len(dict_columns) > 0:
        new_columns = []
        
        for col in dict_columns:
            print(f"flattening: {col}")
            # explode dictionaries horizontally, adding new columns
            horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}.')
            horiz_exploded.index = df.index
            df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
            new_columns.extend(horiz_exploded.columns) # inplace
        
        for col in list_columns:
            print(f"exploding: {col}")
            # explode lists vertically, adding new columns
            df = df.drop(columns=[col]).join(df[col].explode().to_frame())
            # Prevent combinatorial explosion when multiple
            # cols have lists or lists of lists
            df = df.reset_index(drop=True)
            new_columns.append(col)
        
        # check if there are still dict o list fields to flatten
        s = (df[new_columns].applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df[new_columns].applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()
        
        print(f"lists: {list_columns}, dicts: {dict_columns}")
        
    print(f"final shape: {df.shape}")
    print(f"final columns: {df.columns}")
    return df

#TODO need to make date range generic so this is easier to share
# This contains the GraphQL query and request logic
def get_all_tournies_for_fourth_pr_season(auth_token, coords, radius, num_per_page):

  # This is the query
  graphql_query = """
query BayNorCalTournaments($page: Int, $perPage: Int, $coordinates: String!, $radius: String!) {
  tournaments(
    query: {
    page: $page
    perPage: $perPage
    filter: {
      location: {
        distanceFrom: $coordinates,
        distance: $radius
      },
      afterDate: 1696032000
      beforeDate: 1704153540
    }
    sortBy:"startAt"
  }) {
    nodes {
      id
      name
      city
      slug
      startAt
      events {
        slug
        numEntrants
        videogame {
          name
        }
      }
    }
  }
}
"""
  tournies = []

  # This range here is used because the results from startGG are paginated, i.e. they don't show all the results,
  #   they're shown in pages. If there were 100 resuls, and we could only see 10 per page, then we would need to go through
  #   and make ten seperate requests to see all the tournaments we need
  for i in range(1, 10):
    variables = {
        "page": i,
        "perPage": num_per_page,
        "coordinates": coords,
        "radius": "50mi"
    }
    data = {"query" : graphql_query, "variables": variables}
    json_data = json.dumps(data)
    auth_header = auth_token
    header = {'Authorization': auth_header}  


    # Extracting & making the the actual response to startgg
    response = requests.post(url=request_url, headers=header, data=json_data)
    json_resp = json.loads(response.text)
    curr_tournies_page = json_resp['data']['tournaments']['nodes']

    # The number printed should be equal num_per_page until we reach the last page, then it should be 0.
    #   If it's not then there might be some errors/potentially getting rate limited. TODO Add validation if
    #   this is gonna be used for something actually important
    print("Number of tournies in page is:" + str(len(curr_tournies_page)))

    # Add the current page of tournaments that we've queried into our local set. 
    tournies += curr_tournies_page
  return tournies


tournies = []

SF_BASED_COORDS = "37.77151615492457, -122.41563048985462"
SF_RADIUS = "70mi"

SAC_BASED_COORDS = "38.57608096237729, -121.49183616631059"
SAC_RADIUS = "40mi"

NUM_PER_PAGE = 50

#Start gg API lets us get tournaments with a coordinate radius. We do one for SF to cover the bay area
# and another for Sac to cover sac. 
bay_tournies = get_all_tournies_for_fourth_pr_season(AUTH_TOKEN, SF_BASED_COORDS, SF_RADIUS, NUM_PER_PAGE)
sac_tournies = get_all_tournies_for_fourth_pr_season(AUTH_TOKEN, SAC_BASED_COORDS, SAC_RADIUS, NUM_PER_PAGE)
tournies = sac_tournies + bay_tournies

np_tournies = pd.DataFrame(tournies).explode('events')

###
#  The actual schema of a tournament is a little complicated. See the API reference doc: https://developer.start.gg/reference/tournament.doc
# Essentially tournamnets contain lists of events, which are their own objects. This makes filtering and viewing
# along event parameters a little difficult, so instead we explode the object out into its own columns to make it
# easier to work with
flat_tournies = flatten_nested_json_df(np_tournies)

# In this we do a few things
#  1. Find the events that we care about (e.g. Ultimate)
#  2. Filter to only include potentially PR sanctioned events (attendance > 16)
ult_tournies = flat_tournies[
    (flat_tournies['events.videogame.name'] == 'Super Smash Bros. Ultimate') & (flat_tournies['events.numEntrants'] >= 16) 
]



# This next section of the code is p much all just renaming stuff to make things a little bit more understadable, visually

# The event slug actually is the start gg url suffix
ult_tournies = ult_tournies.rename(columns={"events.slug":"startgg_url"})
ult_tournies['startgg_url'] = 'start.gg/' + ult_tournies['startgg_url'].astype('str')

# StartGG starttime is unix timestamp. Something to keep in mind is that  datetime.fromtimestamp automatically converts it 
#   to your local timezone, so be just be aware/careful if that matters.
ult_tournies['Event Date'] = ult_tournies['startAt'].map(lambda x:  datetime.fromtimestamp(x).strftime('%Y-%m-%d'))

# Some extra transformations to present the data in a manner more similar to what is asked in the braacket upload
#   Url's are usually of the form of start.gg/tournament/TOURNAMENT_ID/event/EVENT_ID
#   We can use some array tricks to extract the 3rd to last item and last item from the startgg url that we constructed earlier
#   to get the TOURNAMENT_ID and EVENT_ID fields for the tournament. The negative numbers are to get the correct index, the [0] 
#   is to unwrap the item so the output is raw data rather than a list, e.g. "EVENT_ID" instead of ["EVENT_ID"]
ult_tournies['StartGG TOURNAMENT_ID'] = ult_tournies['startgg_url'].map(lambda url: url.split("/")[-3:-2][0])
ult_tournies['StartGG EVENT_ID'] = ult_tournies['startgg_url'].map(lambda url: url.split("/")[-1:][0]) 

#The coordinate ranges from sac and the bay could overlap in the area, so we just drop the duplicates just to be safe based on the
# URL for the event, which should be unique.
ult_tournies = ult_tournies.drop_duplicates('startgg_url', keep='first')

# This is so I can save the csv to my local location. This will be in the same directory as wherever you have this file
cwd = os.getcwd()
path = cwd + "/new.csv"
print(path)
ult_tournies.to_csv(path)

print(ult_tournies)