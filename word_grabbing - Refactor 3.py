#-------------------------------------------------
#   Import Libraries
#-------------------------------------------------

# SQLite3 is used to create and manage the databases
import sqlite3

# Spacy is used to tokenize the words. nlp loads the large English model, which is used to tokenize the words.
# The large model is used because it has the most vectors available, which increases simularity accuracy.
# The model is also used to check for OOV words and lemma extraction.

import spacy
nlp = spacy.load("en_core_web_lg")

#-------------------------------------------------
#   Import modules
#-------------------------------------------------

# Simplify adds a few little niceties in centralized spot.
from Important.simplify import *

#-------------------------------------------------
#   Settings
#-------------------------------------------------

# The titles of the text files to be processed
titles = ['peterpan']                                                      #, 'callofthewild', 'junglebook', 'frankenstein']

# The size of each chunk of words to be processed at a time.
    # Larger batch size = faster processing, but more memory usage.
    # Smaller batch size = slower processing, but less memory usage.

batch_size = 5000

#-------------------------------------------------
#   Word Database
#-------------------------------------------------

# Create/connect to the word database
word_db = sqlite3.connect('word_db.db')
word_cursor = word_db.cursor()

# Create the word tables
word_cursor.execute("CREATE TABLE IF NOT EXISTS words (word TEXT PRIMARY KEY)")
word_cursor.execute("CREATE TABLE IF NOT EXISTS oov (word TEXT PRIMARY KEY)")
word_cursor.execute("CREATE TABLE IF NOT EXISTS lemmas (word TEXT PRIMARY KEY)")
word_cursor.execute("CREATE TABLE IF NOT EXISTS nonlemmas (word TEXT PRIMARY KEY)")
word_db.commit()

#-------------------------------------------------
#   Token Database
#-------------------------------------------------

# Create/connect to the word database
token_db = sqlite3.connect('token_db.db')
token_cursor = token_db.cursor()

# Create the token table
token_cursor.execute("CREATE TABLE IF NOT EXISTS tokens (word TEXT PRIMARY KEY, token BLOB)")
token_cursor.execute("CREATE INDEX IF NOT EXISTS token_index ON tokens (word)")
token_db.commit()

#-------------------------------------------------
#   Simularity Database
#-------------------------------------------------

# Create/connect to the simularity database
sim_db = sqlite3.connect('sim_db.db')
sim_cursor = sim_db.cursor()

# Create the simularity table
sim_cursor.execute("""
    CREATE TABLE IF NOT EXISTS simularity (
    word1 TEXT, 
    word2 TEXT, 
    sim REAL, 
    PRIMARY KEY (word1, word2),
    CHECK (word1 < word2)
    );
""")
sim_cursor.execute("CREATE INDEX IF NOT EXISTS word1_index ON simularity (word1)")
sim_cursor.execute("CREATE INDEX IF NOT EXISTS word2_index ON simularity (word2)")
sim_db.commit()


# Get all words from the text files
def get_words(titles:list=titles) -> tuple[list, list, list]:
    '''Pulls all the words from the specified text files and returns them as a list.'''

    #! TO DO: create a nested batch database save function so I'm not repeating code

    # Create a new set to store the words. Sets prevent duplicates.
    words = set()

    # Loop through all the text files
    for title in titles:
        with open(f'{title}.txt', 'r', encoding='utf-8') as file:

            # Read the text file and split it into words. Replace all hyphens with spaces, as they are often used to connect words.
            text = file.read()
            text = text.replace('-', ' ')
            all_words = text.split()
            
            # Loop through all the words in the text file
            for i, word in enumerate(all_words):
                if i % 125 == 0:
                    string, end = return_loading_string(i, len(all_words))
                    print(f"{clear_line}Caching words in {title}.txt{string}", end=end)
                
                # Get rid of deadspace, punctuation, and make the word lowercase to normalize it
                word = word.strip('.,!?";.”-“,(—:)—“’‘').lower()
                word = word.replace('’s', "")
                word = word.replace('’t', "'t")
                word = word.replace('’ll', "'ll")
                word = word.replace('’ve', "'ve")
                word = word.replace('’re', "'re")
                word = word.replace('’d', "'d")
                word = word.replace('’m', "'m")
                word = word.replace('’em', "'em")
                word = word.replace('’clock', "'clock")
                word = word.replace('’cos', "'cos")
                word = word.replace('’twas', "'twas")

                # Add the processed word to the new words set. It being a set takes care of preventing duplicates.
                words.add(word)

    # Initialize the new words and known words sets, and the batch set
    new_words = set()
    known_words = set()
    batch = set()

    # Convert words to a list so they can be indexed
    words = list(words)

    for i, word in enumerate(words):
        # Add the word to the batch
        batch.add(word)

        # Every batch_size words, add new words to the database. The ternary operators adapt the batch size to the remaining words automatically.
        if (i % batch_size == 0 if (len(words) - i) > batch_size else i % 25 == 0 if (len(words) - i) > 25 else True) and i != 0:
            # Grab already known words from the database
            placeholders = ','.join('?' * len(batch))
            word_cursor.execute(f"SELECT word FROM words WHERE word IN ({placeholders})", list(batch))

            # Add the known words to the known words set, so they're not added to the database again.
            for word, in word_cursor.fetchall():
                known_words.add(word)
            
            # Get only the new words by subtracting the known words from the batch
            words_to_process = batch - known_words
            new_words.update(words_to_process)

            # Save the new words to the database
            for i2, word in enumerate(words_to_process):
                if i2 % 5 == 0:
                    dots, percent, loading_bar, end = return_loading_string(i + i2, len(words) + len(words_to_process), seperate_string=True)
                    print(f"{clear_line}Saving {word}{dots}{" " * (15 - len(word))}{percent} {loading_bar}", end=end)
                word_cursor.execute("INSERT OR IGNORE INTO words (word) VALUES (?)", (word,))

            batch = set()

    print(f"{clear_line}Finished pulling {len(words)} words from {len(titles)} text {'files' if len(titles) > 1 else 'file'} {green}✔{white}" if len(words) > 0 else f"No words could be found {red}✘{white}", end="\n")
    return list(new_words), list(known_words), words

def tokenize_words(words:list, batch_size:int=batch_size) -> tuple[list, list, list]:

    words = words.copy()
    # If there are no new words, return an empty list and print a message.
    if len(words) == 0:
        print(f"{clear_line}No new words found {green}✔{white}", end="\n")
        return [], [], []

    # Check if the words are already known to be out-of-vocabulary
    placeholders = ','.join('?' * len(words))
    word_cursor.execute(f"SELECT word FROM oov WHERE word IN ({placeholders})", list(words))
    oov_words = set(word_cursor.fetchall())

    # Print how many/if any out-of-vocabulary words were found, and remove them from the list of words for processing.
    print(f"{clear_line}Ignored {len(oov_words)} known out of vocabulary words {green}✔{white}" if len(oov_words) > 0 else f"No cached {purple}OOV{white} words found {red}✘{white}", end="\n")
    for word in oov_words:
        words.remove(word[0])

    # Check if the words are already tokenized
    placeholders = ','.join('?' * len(words))
    token_cursor.execute(f"SELECT word FROM tokens WHERE word IN ({placeholders})", list(words))
    tokenized_words = token_cursor.fetchall()

    # Print how many/if any already tokenized were found, and remove them from the list of words for processing.
    print(f"{clear_line}Ignored {len(tokenized_words)} stored tokenized words {green}✔{white}" if len(tokenized_words) > 0 else f"No stored {bright_yellow}tokens{white} found {red}✘{white}", end="\n")
    for word in tokenized_words:
        words.remove(word[0])
    
    # Initialize the new tokens, tokenized words, and new out-of-vocabulary words sets
    new_oov = set()
    batch = set()
    tokenized_words = set()
    new_tokens = set()

    # Keep track of if an error was found, which will change the output message.
    error_found = False
    for i, word in enumerate(words):
        batch.add(word)
        if (i % batch_size == 0 if (len(words) - i) > batch_size else i % 25 == 0 if (len(words) - i) > 25 else True) and i != 0:
            placeholders = ','.join('?' * len(batch))
            
            
            for word in token_cursor.fetchall():
                tokenized_words.add(word)

            for i2, word in enumerate(batch):
                if i2 % 5 == 0:
                    dots, percent, loading_bar, end = return_loading_string(i + i2, len(words) + len(batch), seperate_string=True)
                    print(f"{clear_line}Tokenizing {word}{dots}{" " * (15 - len(word))}{percent} {loading_bar}", end=end)
                if word.isalpha():
                    doc = nlp(word)
                    if doc.has_vector:
                        token_cursor.execute("INSERT OR IGNORE INTO tokens (word, token) VALUES (?, ?)", (word, nlp(word).to_bytes()))
                        new_tokens.add(word)
                    else:
                        if not error_found:
                            error_found = True
                            print(f"{red}Warning: {word} has no tokens. Skipped.{white}{clear_line}{down}", end="\r")
                        else:
                            print(f"{up}{red}Warning: {word} has no tokens. Skipped.{white}{clear_line}{down}", end="\r")
                        word_cursor.execute("INSERT OR IGNORE INTO oov (word) VALUES (?)", (word,))
                        new_oov.add(word)
            token_db.commit()
            word_db.commit()

            batch = set()

    # Print the outcome. 
    if not error_found:
        print(f"{clear_line}Finished tokenizing {len(words)} words {green}✔{white}" if len(words) > 0 else f"{up}{clear_line}All words were tokenized {green}✔{white}", end="\n")     
    else:
        print(f"{up}{clear_line}{up}{clear_line}{up}{clear_line}Saved {len(new_oov)} new out of vocabulary words into the database {green}✔{white}{down}")
        print(f"{up}{clear_line}Finished tokenizing {len(words)} words {green}✔{white}{down}{clear_line}{up}" if len(words) > 0 else f"{up}{clear_line}All words were tokenized {green}✔{white}{down}{clear_line}{up}", end="\n")
        print(f"{down}{clear_line}{up}", end="\r")

    return list(new_tokens), list(tokenized_words), list(new_oov)

def get_lemmas(words:list) -> list:

    # Create placeholders for both the lemmas and nonlemma queries
    placeholders = ','.join('?' * len(words))

    # Get all the lemmas
    word_cursor.execute(f"SELECT word FROM lemmas WHERE word IN ({placeholders})", list(words))
    known_lemmas = set(word_cursor.fetchall()) if word_cursor.fetchall() != [] else set(word_cursor.execute("SELECT word FROM lemmas").fetchall())

    # Get all the nonlemmas
    word_cursor.execute(f"SELECT word FROM nonlemmas WHERE word IN ({placeholders})", list(words))
    nonlemmas = set(word_cursor.fetchall())

    # Exclude words that have already been processed
    excluded = set(word[0] for word in known_lemmas | nonlemmas)
    new_words = list(set(words) - excluded)

    print(f"{clear_line}Ignored {len(excluded)} words with lemma data {green}✔{white}" if len(excluded) > 0 else f"No cached lemma data found {red}✘{white}", end="\n")

    # Get the tokens for all the new words
    placeholders = ','.join('?' * len(new_words))
    token_cursor.execute(f"SELECT word, token FROM tokens WHERE word IN ({placeholders})", list(new_words))
    tokens = token_cursor.fetchall()
    
    # Initialize the lemmas and batch sets
    new_lemmas = set()
    batch = set()
    
    # Get the lemmas for all tokenized words
    for i, (word, doc_bytes) in enumerate(tokens):

        # Add the word to the batch
        batch.add((word, doc_bytes))
        
        # Every batch_size words, add new lemmas to the database. The ternary operators adapt the batch size to the remaining words automatically.
        if (i % batch_size == 0 if (len(tokens) - i) > batch_size else i % 25 == 0 if (len(tokens) - i) > 25 else True) and i != 0:
            
            # Parse the batch's word/doc byte pairs
            for i2, (word, doc_bytes) in enumerate(batch):

                # Every 5 words, update the loading string
                if i2 % 5 == 0:
                    dots, percent, loading_bar, end = return_loading_string(i + i2, len(tokens) + len(batch), seperate_string=True)
                    print(f"{clear_line}Saving lemma for {word}{dots}{" " * (15 - len(word))}{percent} {loading_bar}", end=end)
                
                if word.isalpha():
                    # Reassemble the doc/token
                    doc = spacy.tokens.Doc(nlp.vocab).from_bytes(doc_bytes)
                    token = doc[0]

                    lemma = token.lemma_
                    # Check if the word is a lemma
                    if token.lemma_ == word:

                        # If the word is a lemma, add it to the lemmas set, but only if it's not already in there
                        if word not in known_lemmas:
                            new_lemmas.add(word)
                            word_cursor.execute("INSERT OR IGNORE INTO lemmas (word) VALUES (?)", (lemma,))

                    # If the word is not a lemma, add it to the nonlemmas so it's not checked again
                    else:
                        word_cursor.execute("INSERT OR IGNORE INTO nonlemmas (word) VALUES (?)", (word,))
            
            # Commit the changes to the database
            word_db.commit()
            batch = set()
    
    # Save all cached  to the database
    for i, lemma in enumerate(new_lemmas):
        if i % 5 == 0:
            dots, percent, loading_bar, end = return_loading_string(i, len(new_lemmas), seperate_string=True)
            print(f"{clear_line}Saving lemma for {lemma}{dots}{" " * (15 - len(lemma))}{percent} {loading_bar}", end=end)
    word_db.commit()

    print(f"{up}{clear_line}Finished getting lemmas for {len(new_lemmas)} words {green}✔{white}" if len(new_lemmas) > 0 else f"{up}{clear_line}No new lemmas were found {green}✔{white}", end="\n")
    return list(new_lemmas), list(known_lemmas), list(known_lemmas) + list(new_lemmas)

# Here goes the one I'm terrified of the most... Simularity checking. Here goes nothing.
def check_simularity(words:list):
    sim_cursor.execute("ATTACH DATABASE 'word_db.db' AS word_db")
    # Get all the lemmas
    placeholders = ','.join('?' * len(words))

    # Get every word in the simularity database. Used to check if there's any stragglers with no simularity data, which is impossible once the first word is fully done.
    sim_cursor.execute("""
    SELECT MIN(occurrences) AS min_matches
    FROM (
        SELECT word, COUNT(*) AS occurrences
        FROM (
            SELECT word1 AS word FROM simularity
            UNION ALL
            SELECT word2 AS word FROM simularity
        ) subquery
        GROUP BY word
    )
    """)
    unique_word_count = sim_cursor.fetchone()[0]

    lemmas = word_cursor.execute("SELECT * FROM lemmas").fetchall()
    lemmas = [lemma[0] for lemma in lemmas]

    # Get every word in the simularity database. Used to check if there's any stragglers with no simularity data, which is impossible once the first word is fully done.
    sim_cursor.execute("""
        SELECT word
        FROM (
            SELECT word1 AS word FROM simularity
            UNION ALL
            SELECT word2 AS word FROM simularity
        ) subquery
        GROUP BY word
        HAVING COUNT(*) < (((SELECT COUNT(*) FROM word_db.lemmas) - 1) / 2);
    """)

    not_done = set([word[0] for word in sim_cursor.fetchall()]) if unique_word_count != (0 or None) else set(lemmas)
    not_done = sorted(list(not_done))

    new_similarities = 0
    # Compare all the lemmas to each other
    for i, word in enumerate(not_done):
        dots, percent, loading_bar, end = return_loading_string(i, (len(lemmas) / 2), seperate_string=True)
        print(f"{clear_line}{up}Checking simularities for {word}{dots}{" " * (15 - len(word))}{percent} {loading_bar}{down}", end=end)

        # Reassemble the doc/token
        doc_bytes = token_cursor.execute("SELECT token FROM tokens WHERE word = ?", (word,)).fetchone()[0]
        doc = spacy.tokens.Doc(nlp.vocab).from_bytes(doc_bytes)
        token = doc[0]
        needs_to_match = set(word[0] for word in sim_cursor.execute("""
            SELECT word 
            FROM word_db.lemmas
            WHERE word NOT IN (
                SELECT word2 FROM simularity WHERE word1 = ? 
                UNION 
                SELECT word1 FROM simularity WHERE word2 = ?
            )
            """, (word, word)).fetchall())
        needs_to_match = sorted(list(needs_to_match))

        # Compare the token to all other tokens
        for i2, word2 in tuple(enumerate(needs_to_match))[i+1:]:
            if i2 % 10 == 0:
                dots, percent, loading_bar, end = return_loading_string(i2, len(needs_to_match), seperate_string=True)
                print(f"{clear_line}Checking simularity with {word2}{dots}{" " * (16 - len(word2))}{percent} {loading_bar}", end=end)

            # Check if the simularity has already been calculated, and if not, calculate it.
            pair = [word, word2]
            pair = sorted(pair)
            key = tuple(pair)
            sim_cursor.execute("SELECT sim FROM simularity WHERE word1 = ? AND word2 = ?", (key[0], key[1]))
            if not sim_cursor.fetchone():
                # Reassemble the doc/token
                doc_bytes2 = token_cursor.execute("SELECT token FROM tokens WHERE word = ?", (word2,)).fetchone()[0]
                doc2 = spacy.tokens.Doc(nlp.vocab).from_bytes(doc_bytes2)
                token2 = doc2[0]

                # Calculate the simularity
                sim = round(token.similarity(token2), 5)
                new_similarities += 1

                # Save the simularity to the database
                sim_cursor.execute("INSERT OR IGNORE INTO simularity (word1, word2, sim) VALUES (?, ?, ?)", (key[0], key[1], sim))
                new_similarities += 1

            if i2 % batch_size == 0:
                sim_db.commit()

        sim_db.commit()
    
    return new_similarities


def get_matches(word, sim=0.5):
    matches = []
    sim_cursor.execute("SELECT word1, word2, sim FROM simularity WHERE word1 = ? OR word2 = ?", (word, word))
    retrieved_matches = sim_cursor.fetchall()
    for match in retrieved_matches:
        if match[2] > float(sim):
            matches.append(match[0] if match[0] != word else match[1])
    longest = max([len(match) for match in matches])
    return matches, longest
        



def main():
    # Start up
    clear()
    hide_cursor()

    # PRAGMA
    sim_cursor.execute("PRAGMA synchronous = OFF")
    sim_cursor.execute("PRAGMA journal_mode = MEMORY")
    sim_cursor.execute("PRAGMA temp_store = MEMORY")
    sim_cursor.execute("PRAGMA cache_size = 10000")

    # Process texts
    new_words, known_words, all_words = get_words()
    new_tokens, known_tokens, new_oov = tokenize_words(all_words)

    # get_lemmas MUST be done AFTER tokenize_words, because it skips over words that have no tokens. Which would be, like, all of them. Tokenize them first.
    new_lemmas, known_lemmas, all_lemmas = get_lemmas(list(set(new_words) - set(new_oov)))
    new_simularies = check_simularity(all_words)


    # Close up
    print(f"{clear_line}Closing databases...", end="\r")

    # Word database
    word_db.commit()
    word_db.close()

    
    # Token database
    token_db.commit()
    token_db.close()

    sim_cursor.execute("PRAGMA synchronous = NORMAL")
    
    if len(new_words) + len(new_tokens) + len(new_oov) + len(new_lemmas) + new_simularies > 0:       
        print(f"{clear_line}{up}{clear_line}{up}{clear_line}{up}{clear_line}{up}{clear_line}{up}{clear_line}{up}", end="\n")
        print(f"{clear_line}Finished {green}✔{white} {light_grey}")
        print(f"Processed {cyan}{len(all_words)}{white} words, found and cached:")
        print(f"  - {cyan}{len(new_words) if len(new_words) > 0 else "no"}{white} new {blue}words{white}")
        print(f"  - {cyan}{len(new_tokens) if len(new_tokens) > 0 else "no"}{white} new {bright_yellow}tokens{white}")
        print(f"  - {cyan}{len(new_oov) if len(new_oov) > 0 else "no"}{white} new {purple}out-of-vocaulary words{white}") 
        print(f"  - {cyan}{len(new_lemmas) if len(new_lemmas) > 0 else "no"}{white} new {light_green}lemmas{white}.", end=f"{white}\n")
        print(f"  - {cyan}{new_simularies if new_simularies > 0 else "no"}{white} new {grey}simularities{white}.", end=f"{white}\n")
    else:
        print(f"{clear_line}{up}{clear_line}{up}{clear_line}{up}{clear_line}{up}{clear_line}{up}{clear_line}{up}", end="\n")
        print(f"{clear_line}Processed {cyan}{len(all_words)}{white} words from {cyan}{len(titles)}{white} texts. No changes were detected {green}✔{white}", end="\n")
    show_cursor()
    input("Press any key to continue.")
    clear()
    word = input("Enter a word to check for simularities: ")
    sim = input("Enter a simularity threshold: ")
    while True:
        matches, longest = get_matches(word, sim)
        print(f"{clear_line}Matches for {word} at {sim} simularity:")
        for i in range(0, len(matches), 5):
            words = " ".join([f"[{word}]{" " * (longest + 2 - len(word)) if longest - 2 < (20 - len(word)) else " " * 20 - len(word)}"  for word in matches[i:i+5]])
            print(f"{words}", end="\n")

        print()
        word = input("Enter a word to check for simularities: ")
        sim = input("Enter a simularity threshold: ") 
        clear()

    
    


if __name__ == '__main__':
    main()
