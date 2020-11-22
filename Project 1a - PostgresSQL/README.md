<h1>ETL Pipeline for Sparkify</h1>
<h3>This is ETL process built for Sparkify project  to analyze the data they've been collecting on songs and user activity on their new music streaming app</h3>
<p>The fact table <strong><em>'songplays'</em></strong> and dimension tables schema <strong><em>'users','songs','artists' and 'time'</em></strong> are created in sql_queries.py<br>
The ETL process in Notebook <em>'etl.ipynb'</em> is implemented for a single JSON file in song and log data folder <br>
<em>'etl.py'</em>  ETL file is processed for all the files in song and log data folders<br>
The final table <strong><em>'songplays'</em></strong> fetches the song and artist information from song and log files<br>
The 'song id' and 'artist id' from the first song data set which is loaded on the songs and artists tables is to load<br>
the fact table, based on the values from the log file(song,artist and length), we get the corresponding values from the song file (songs and artist tables)<br>
Perform following steps to get data into the tables <br>
<ol>
<li>python create_tables.py</li>
    <p>This will drop any existing tables and create the fact and dimension tables</p>
<li>run etl.ipynb for a single JSON file</li>
     <p>This will insert records using single song and log JSON in all fact and dimension tables<br>
      Run test.ipynb to check if the data is inserted in the tables</p>
<li>python etl.py</li>
    <p>This will process all the JSON files in the song and log data folders and insert records into fact and dimension tables<br>
    Run test.ipynb to check if all the data is inserted in the tables</p>
</ol>
<strong>The final table 'songplays'  have the songs and user activity ready for the Sparkify analytical team displayed as below</strong>
<img src="Screenshots/song_plays_table.PNG" height="400" alt="Screenshot"/>
</p> 