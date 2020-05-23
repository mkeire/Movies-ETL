try:

    # Importing dependencies

    import json

    import pandas as pd

    import numpy as np

    import re

    import time

    from sqlalchemy import create_engine

    from config import db_password
    
except:
    
    print("Module not found. Please install module and execute again.")

try:
    
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    engine = create_engine(db_string)

except:

    print("Invalid database password.")
    
try:

    # Ask user for file directory
    file_dir = input('Please enter direct path for file directory... ')

    with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
        kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
        ratings = pd.read_csv(f'{file_dir}ratings.csv')

except:
    
    print("Please execute again and enter the correct direct path.")

else:
    
    def movies_etl(wiki_movies_raw, kaggle_metadata, ratings): 
        '''Assume future files will follow current files' respective formats, e.g. wiki_movies will have a 'Director'
        or 'Directed by' column.'''
        # Collect only movies with director information
        wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]
        
        def clean_movie(movie):
            
            movie = dict(movie)
            alt_titles = {}
            '''Combine alternate titles into one list. Assume these are the only other languages based
            on the current dataset. Add more if needed.'''
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune–Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles
            
            # Merge and rename columns
            def change_column_name(old_name, new_name):
                
                if old_name in movie:
                    movie[new_name] = movie.pop(old_name)
            change_column_name('Adaptation by', 'Writer(s)')
            change_column_name('Country of origin', 'Country')
            change_column_name('Directed by', 'Director')
            change_column_name('Distributed by', 'Distributor')
            change_column_name('Edited by', 'Editor(s)')
            change_column_name('Length', 'Running time')
            change_column_name('Original release', 'Release date')
            change_column_name('Music by', 'Composer(s)')
            change_column_name('Produced by', 'Producer(s)')
            change_column_name('Producer', 'Producer(s)')
            change_column_name('Productioncompanies ', 'Production company(s)')
            change_column_name('Productioncompany ', 'Production company(s)')
            change_column_name('Released', 'Release Date')
            change_column_name('Release Date', 'Release date')
            change_column_name('Screen story by', 'Writer(s)')
            change_column_name('Screenplay by', 'Writer(s)')
            change_column_name('Story by', 'Writer(s)')
            change_column_name('Theme music composer', 'Composer(s)')
            change_column_name('Written by', 'Writer(s)')
            return movie

        # Cleaning movie data to merge and rename duplicate information to get English movie versions
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        
        wiki_movies_df = pd.DataFrame(clean_movies)

        # Pull IMDB identication numbers
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

        # Remove duplicate movies determined by identification numbers
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

        # Remove movies that have null box office values
        box_office = wiki_movies_df['Box office'].dropna()

        # Retrieve box office data that are not strings and convert to a string
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

        '''Matching how box office data is formed and ignoring box office revenues not stated in $USD.
        Assume data left out is negligible.'''
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

        '''Replacing box office ranges with the higher end range,
        assuming the higher end range is accurate.'''
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        def parse_dollars(s):
            
            if type(s) != str:
                return np.nan
            
            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                
                # remove dollar sign and " million"
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and multiply by a million
                value = float(s) * 10**6
                return value
            
            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                
                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                # convert to float and multiply by a billion
                value = float(s) * 10**9
                return value
            
            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                
                # remove dollar sign and commas
                s = re.sub('\$|,','', s)
                # convert to float
                value = float(s)
                return value
            
            else:
                
                return np.nan
        
        # Converting box office data to floats
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Drop movies with null budget data
        budget = wiki_movies_df['Budget'].dropna()

        # Convert budget data to strings
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

        # Remove lower end budget data given in a range
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Extract box office information and convert to floats
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Drop null release date data and convert to strings
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Date forms to capture
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'

        # Convert to date type
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(
        f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')
        [0], infer_datetime_format=True)

        # Drop null running time data and convert to strings
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Capture running time data in its different forms
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

        # Convert running time data to floats and fill nulls with zero
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

        # Drop Kaggle movies that are labelled adult
        # Assume movies are correctly categorized
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

        # Convert Kaggle video data to Boolean type
        # Assume data is a string in form of true or false
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

        # Convert remaining data in Kaggle to proper data types
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

        # Convert rating's unix time stamps to dates
        # Assume rating dates will be given as a Unix timestamp
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

        # Merge data based on the movie's IMDB ID
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

        # Assume Kaggle movie titles are correct
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

        # Use Kaggle over Wiki data and fill in zeros with Wiki data
        # Assume Wiki data is correct and can fill in missing Kaggle data
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
            df.drop(columns=wiki_column, inplace=True)

        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

        # Reorder columns
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]

        # Rename columns
        movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)
        
        # Group ratings by ID and ratings on count
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')

        # Rename rating columns
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        # Merge ratings with movies on Kaggle ID
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

        # Fill in zeros for movies with no rating
        # Assume no rating means a zero
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        try:
            # Export movie data to SQL
            movies_df.to_sql(name='movies', con=engine, if_exists=input('Do you wish to *replace* or *append* the movies data?'))

            # Export ratings data to SQL using progress counter
            rows_imported = 0
            start_time = time.time()
            # Set chunksize for export to show data shards exported
            
            data_entry = input('Do you wish to *replace* or *append* the ratings data?')
            for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
                print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
                data.to_sql(name='ratings', con=engine, if_exists=data_entry)
                rows_imported += len(data)
                # Show elapsed time
                print(f'Done. {time.time() - start_time} total seconds elapsed')
        except:
            print('Please execute again and enter *append*, *replace*, or *fail*.')
            
movies_etl(wiki_movies_raw, kaggle_metadata, ratings)