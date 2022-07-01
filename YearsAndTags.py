import pandas as pd
import datetime


# Removes the specified characters from the given string
# <string> The string  
def FormatYearString(string):
    return string.replace(")", "").replace("\"", "").replace(" ", "").replace("–", "").replace("-", "")

# Converts a string to a year
# <string> The string  
def StringToYear(string):
   
    # If it is a year up to present...
    if ("–" in string or "-" in string):

        # Returns the current year
        return datetime.date.today().year

    # Formats the string to a year
    split = FormatYearString(string)    

    # Returns the year as an integer
    return int(split)

# Indicates whether the string is a year
# <string> The string  
def IsYear(string):

    # Formats the string to a year
    split = FormatYearString(string) 

    # If the string has length not equal to 4...
    if len(split) != 4:

        # Return false
        return False

    # Sets boolean as true if the split is numeric else false
    boolean = split.isnumeric()

    # Returns the boolean
    return boolean

# Creates and returns a data fame from the given CSV file
# <csvPath> The csv path  
def CreateDataFrame(csvPath):

    # Creates and returns a data fame from the given CSV file
    return pd.read_csv(csvPath)

# Gets the years from the movies column of the given data frame
# <df> The data frame  
def GetYears(df):

    # Gets the movie titles from the data frame
    movieTitles = df[['movieId', 'title']]

    movieYears = []

    # For each row...
    for i in df.index:
        title = df.at[i, "title"]
        titleSplit = title.split("(")

        # If any string in the title is not a years...
        if any(IsYear(split) for split in titleSplit) == False:
            # Add none to the list
            movieYears.append(None)
        # Else...
        else:
            # Gets the first string that is a year
            split = next(year for year in titleSplit if IsYear(year) == True)
            # Parses the formatted string to an int 
            year = StringToYear(split)
            # Adds the year to the list
            movieYears.append(int(year))

    # Returns the list with all the movie years
    return movieYears

# Gets the number the tags are referenced in the "tags.csv" ...
# File and adds them the data frame of the "genome_tags.csv" file
# <tagsDf> The data frame of the tags
# <genomeTagsDf> The data frame to store the references
def GetTagReferences(tagsDf, genomeTagsDf):
    
    # Creates a new list for the tag references
    tagReferences = []
    # For each row in the data frame...
    for i in genomeTagsDf.index:
        # Get the value of the column "tag"
        tag = genomeTagsDf.at[i, "tag"]
        # Creates a new data frame with the rows that contain the tag
        referencesDf = tagsDf.loc[tagsDf["tag"] == tag]
        # Gets the count of the rows in the new data frame
        referencesCount = len(referencesDf)
        # Adds the number of references to the list
        tagReferences.append(referencesCount)
    # Returns the list
    return tagReferences


# moviesDf = CreateDataFrame('CSV\movie.csv')

# movieYears = GetYears(moviesDf)

# moviesDf.insert(loc = 2, column = "year", value = movieYears)
# # Converts "year" from float to int and replace NaN values
# moviesDf['year'] = moviesDf['year'].fillna(0).astype(int)

# moviesDf.to_csv('CSV\\moviesWithYears.csv', index=False)

# tagsDf = CreateDataFrame('CSV\\tag.csv')

# genomeTagsDf = CreateDataFrame('CSV\\genome_tags.csv')

# tagReferences = GetTagReferences(tagsDf, genomeTagsDf)

# genomeTagsDf["references"] = tagReferences

# genomeTagsDf.to_csv('CSV\\tagsWithReferences.csv',index=False)

# moviesDf = CreateDataFrame('CSV\moviesWithYears.csv')
# moviesDf['rating'] = moviesDf['rating'].fillna(0).astype(float)
# moviesDf.to_csv('CSV\\moviesWithYears.csv', index=False)

# tagsDf = CreateDataFrame('CSV\\tag.csv')
# tagsAndMoviesDf = pd.merge(moviesDf, tagsDf, how = "left", on = "movieId")
# tagsAndMoviesDf.to_csv('CSV\\tagsAndMovies.csv', index = False)