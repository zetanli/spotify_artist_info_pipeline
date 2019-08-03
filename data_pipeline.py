#!/usr/bin/env python
# coding: utf-8

# In[48]:


import luigi
import spotipy.util as util
import spotipy
import mysql.connector
from mysql.connector import Error


# In[49]:


from luigi.contrib.mysqldb import MySqlTarget


# Get some artist data from spotify  
# Store the data into mysql

class userInfo(luigi.Config):
    user = luigi.Parameter()
    id = luigi.Parameter()
    secret = luigi.Parameter()

token = util.prompt_for_user_token(userInfo().user, 
                                   scope='playlist-read-private', 
                                   client_id=userInfo().id, 
                                   client_secret=userInfo().secret,
                                  redirect_uri='https://localhost:8080')

if token:
    spotify = spotipy.Spotify(auth = token)
artist = 'Jay Chou'

class GetArtistInfo(luigi.Task):
    def requires(self):
        return []
    
    def output(self):
        
        return luigi.contrib.mysqldb.MySqlTarget('127.0.0.1', 'spotify', 'root', 'wiwgri-6n',  'artist', 'id')
    
    def run(self):
        
        results = spotify.search(q='artist:'+artist,type = 'artist')
        
        followers = results['artists']['items'][0]['followers']['total']

        popularity =results['artists']['items'][0]['popularity']
        
        cnx = mysql.connector.connect(user='root',password='wiwgri-6n',host='127.0.0.1',database='spotify')
        cursor = cnx.cursor()
        
        add = ("Insert Into artist"
               "(artist,popularity,followers)"
               "VALUES (%(artist)s,%(popularity)s,%(followers)s)")
        
        cursor.execute(add,{'artist':artist,'followers':followers,'popularity':popularity})
    
        cnx.commit()
        cursor.close()
        cnx.close()





# In[55]:


if __name__ == '__main__':
    luigi.run()






