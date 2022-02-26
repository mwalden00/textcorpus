
# Inf2-IADS Coursework 1, October 2019, revised October 2021
# Python source file: index_build.py
# Author: John Longley

# PART A: INDEXING A LARGE SET OF PLAINTEXT FILES
# (template file)


import buffered_io
from buffered_io import *

# Global variables:

CorpusFiles = { 'CAW' : 'Carroll_Alice_in_Wonderland.txt',
                'OEL' : 'Olaudah_Equiano_Life.txt',
                'SLC' : 'Shudraka_Little_Clay_Cart.txt',
                'SCW' : 'Shakespeare_Complete_Works.txt',
                'TWP' : 'Tolstoy_War_and_Peace.txt'
               }
# :each file must be identified by a three-letter code
# :note that the files SCW and TCP are much larger than CAW, OEL, SLC

IndexFile = 'index.txt'
# :name of main index file to be generated

MetaIndex = {'' : 0}
# :dictionary to be populated
# :MetaIndex[k] will give line number in IndexFile for key k

MetaIndexOp = (lambda s: 0)


# Initial scan to determine number of lines in a given text file:

def getNumberOfLines(filename):
    reader = BufferedInput(filename,0.8)
    lines = 0
    chunk = reader.readchunk()
    while chunk != []:
        lines += len(chunk)
        chunk = reader.readchunk()
    reader.close()
    return lines

# Extracting list of words present in a single text line:

def getWords(s):
    t = s   # :could do some translation here to process accented symbols etc.
    words,flg = [],False
    for i in range(len(t)):
        if not flg:
            if t[i].isalpha():
                # potential start of word
                flg=True
                j=i
        else:
            if not t[i].isalpha():
                # potential end of word
                flg=False
                # design decision: ignore words of length < 4:
                if i-j >= 4: 
                    words.append(t[j:i].casefold())
        # :assumes some terminator like \n is present
    return words

# Generation of unsorted index entries for a given textfile:

import math

def generateIndexEntries(filename,filecode,writer):
    numberOfLines = getNumberOfLines(filename)
    digits = int(math.log10(numberOfLines))+1
    padCtrl = '0' + str(digits)  # :controls leading zero padding
    reader = BufferedInput(filename,0.2)
    currline = reader.readln()
    inlineNo = 1
    outlineNo = 0
    while currline != None:
        # process currline:
        words = getWords(currline)
        for w in words:
            writer.writeln(w+':'+filecode+format(inlineNo,padCtrl)+'\n')
        outlineNo += len(words)
        # next line:
        inlineNo += 1
        currline = reader.readln()
    reader.close()
    return outlineNo  # :for testing

def generateAllIndexEntries(entryfile):
    global CorpusFiles
    writer = BufferedOutput(entryfile,0.7)
    outlines = 0
    for filecode in CorpusFiles:
        outlines += generateIndexEntries(CorpusFiles[filecode],filecode,writer)
    writer.flush()
    return outlines

# Sorting the index entries:

import os

def splitIntoSortedChunks(entryfile):
    reader = BufferedInput(entryfile,0.3)
    blockNo = 0
    chunk = reader.readchunk()
    while chunk != []:
        chunk.sort()
        blockfile = open('temp_' + str(blockNo) + '_' + str(blockNo+1),'w',
                         encoding='utf-8')
        # :output file written all at once, so no need for buffering here
        blockfile.writelines(chunk)
        blockfile.close()
        blockNo += 1
        chunk = reader.readchunk()
    reader.close()
    return blockNo

# TODO:
# Add your code here.

def mergeFiles(a, b, c):
    # Make BufferedInput for temp_a_b and temp_b_c files
    reader_a_b = BufferedInput('temp_{}_{}'.format(a,b), .3)
    reader_b_c = BufferedInput('temp_{}_{}'.format(b,c), .3)
    # Read the first lines of each file
    line_a_b = reader_a_b.readln()
    line_b_c = reader_b_c.readln()
    # Make the BufferedOutput for temp_a_c
    writer_a_c = BufferedOutput('temp_{}_{}'.format(a,c), .3)

    # Write lines from each file in order until we reach the end of either file.
    while line_a_b and line_b_c:
        # Compare lines, write line_a_b if less than, otherwise write line_b_c
        # Once a line is written, move on to the next line in the same file
        if line_a_b < line_b_c:
            writer_a_c.writeln(line_a_b)
            line_a_b = reader_a_b.readln()
        else:
            writer_a_c.writeln(line_b_c)
            line_b_c = reader_b_c.readln()

    # Write remaining lines in temp_a_b and close input (note this only occurs iff line_b_c is empty)
    while line_a_b:
        writer_a_c.writeln(line_a_b)
        line_a_b = reader_a_b.readln() 
    reader_a_b.close()

    # Write remaining lines in temp_b_c and close input (note this only occurs iff line_a_b is empty)
    while line_b_c:
        writer_a_c.writeln(line_b_c)
        line_b_c = reader_b_c.readln()
    reader_b_c.close()

    # Clean up
    writer_a_c.flush()
    os.remove('temp_{}_{}'.format(a,b))
    os.remove('temp_{}_{}'.format(b,c))
    return 'temp_{}_{}'.format(a,c)

def mergeFilesInRange(a, c):
    # We start with i = 2^0, and move up in power as we iterate throught temp files
    i = 1
    # Because we need k for any final merges, we define it out of the while loop
    k = a

    # We go through rounds of merging on a,b, and c, first with the difference between a and b of 2 to the power of 0,
    # and moving up in power from there.
    # If i is big enough that a+2*i > c (which can only occurr after 1 full merge round), then we can skip to the final merge.
    # Because a full round of merges will roughly cut the number of temp files in half,
    # we iterate with i as 2 to the power of full merge rounds we've completed.
    while a + 2*i <= c:
        # Reset k
        k = a

        # We will merge the files temp_a_b and temp_b_c with a = k, b = k + i, and c = k + 2*i
        while k <= c-2*i:
            mergeFiles(k,k+i,k+2*i)
            k = k+2*i
        # If k+i < c and the difference between k and c is < 2*i, we account for that and merge
        # on k, k+1, and c
        if k+i < c:
            mergeFiles(k,k+i,c)
        i = i*2

    # If after all of this, k is still less than c and c+2*i > c (e.g. a = 0, k = 8, i = 4, c = 11),
    # we do a final merge on a, k, and c.
    if k < c:
        mergeFiles(a,k,c)
    return 'temp_{}_{}'.format(a,c)



# Putting it all together:

def sortRawEntries(entryfile):
    chunks = splitIntoSortedChunks(entryfile)
    outfile = mergeFilesInRange(0,chunks)
    return outfile

# Now compile the index file itself, 'compressing' the entry for each key
# into a single line:

def createIndexFromEntries(entryfile,indexfile):
    reader = BufferedInput (entryfile,0.4)
    writer = BufferedOutput (indexfile,0.4)
    inl = reader.readln()
    currKey, currDoc, lineBuffer = '', '', ''
    while inl != None:
        # get keyword and ref, start ref list:
        colon = inl.index(':')
        key = inl[:colon]
        doc = inl[colon+1:colon+4] # :three-letter doc identifiers
        j = colon+4
        while inl[j] == '0':
            j += 1
        line = inl[j:-1]
        if key != currKey:
            # new key: start a new line in index
            if key < currKey:
                print('*** ' + key + ' out of order.\n')
            if lineBuffer != '':
                writer.writeln (lineBuffer+'\n')
            currKey = key
            currDoc = ''
            lineBuffer = key + ':'
        if currDoc == '':
            # first doc for this key entry
            currDoc = doc
            lineBuffer = lineBuffer + doc + line
        elif doc != currDoc:
            # new doc within this key entry
            currDoc = doc
            lineBuffer = lineBuffer + ',' + doc + line
        else:
            lineBuffer = lineBuffer + ',' + line
        inl = reader.readln()
    # write last line and clean up:
    writer.writeln (lineBuffer+'\n')
    writer.flush()
    reader.close()

# Generating the meta-index for the index as a Python dictionary:

def generateMetaIndex(indexFile):
    global MetaIndex, MetaIndexOp
    MetaIndex.clear()
    reader = BufferedInput (indexFile,0.9)
    indexline = 1
    inl = reader.readln()
    while inl != None:
        key = inl[:inl.index(':')]
        MetaIndex[key] = indexline
        indexline += 1
        inl = reader.readln()
    reader.close()
    MetaIndexOp = (lambda s: MetaIndex[s])

def buildIndex():
    rawEntryFile = 'raw_entries'
    entries = generateAllIndexEntries (rawEntryFile)
    sortedEntryFile = sortRawEntries (rawEntryFile)
    global IndexFile
    createIndexFromEntries (sortedEntryFile, IndexFile)
    generateMetaIndex (IndexFile)
    os.remove(rawEntryFile)
    os.remove(sortedEntryFile)
    print('Success! ' + str(len(MetaIndex)) + ' keys, ' +
          str(entries) + ' entries.')

# Accessing the index using 'linecache' (random access to text files by line):

import linecache

def indexEntryFor(key):
    global IndexFile, MetaIndex, MetaIndexOp
    try:
        lineNo = MetaIndexOp(key)  # :allows for other meta-indexing schemes
        indexLine = linecache.getline(IndexFile,lineNo)
    except KeyError:
        return None
    colon = indexLine.index(':')
    if indexLine[:colon] == key:
        return indexLine[colon+1:]
    else:
        raise Exception('Wrong key in index line.')

# End of file
