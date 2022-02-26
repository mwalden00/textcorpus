
# Inf2-IADS Coursework 1, October 2019, revised October 2021
# Python source file: perfect_hashing.py
# Author: John Longley

# PART B: A STATE-OF-THE-ART PERFECT HASHING SCHEME
# (template file)

# Adapting a method of Belazzougui, Botelho and Dietzfelbinger (2008)


# Start with very crude 'mod' hashing.
# First, let's read a lowercase word as a base 27 integer:

def toInt(w):
    b = w.encode()
    t = 0
    for i in range(len(b)):
        t = t*27 + b[i] - 96
    return t

# Simple mod hash with some scrambling.
# (We want hashes mod p,p' to be 'independent' when p != p'.)
# We shall take p prime for the outer hash, but not necessarily the inner ones.

def modHash(s,p):
    return (toInt(s)*21436587 + 12345678912345) % p

# Classic 'bucket array' hash table:

def buildHashTable(L,h,r):
    table = [[] for i in range(r)]
    for w in L:
        table[h(w)].append(w)
    return table

def buildModHashTable(L,p):
    return buildHashTable (L, lambda w: modHash(w,p), p)
    # :worth trying out for small L and p
    
# Finding a suitable prime for the outer hash:

def isPrime(n):
    if n%2==0 and n!=2: return False
    else:
        j = 3
        while j*j <= n:
            if n%j==0: return False
            else: j += 2
        else: return True

def prevPrime(n):
    if n%2==0: return prevPrime(n-1)
    elif isPrime(n): return n
    else: return prevPrime(n-2)

# For the mini-hashes, the following very simple enumeration works just fine
# (moduli needn't be prime, but we at least avoid multiples of 2 or 3)
# Results will later be further reduced modulo m (main table size).

def miniHash(m,j):
    d = j*6 + 3000001
    return (lambda w: modHash(w,d) % m)

# TODO:
# Add your code here.

def hashCompress(L, m):
    # We first want to convert L and all it's buckets to a list for consistensy's sake
    L = list(L)
    L = list(map(list,L))

    # We want to keep track of the original indeces of each bucket within L,
    # so our sorted buckets will be paired with their original indeces
    L_sorted = [(L[i],i) for i in range(len(L))]
    L_sorted.sort(reverse=True, key=(lambda xx: len(xx[0])))

    # The indeces of each bucket when sorted.
    L_sorted_i = [i for i in range(len(L))]

    # Define T and R
    # T is a list of booleans of length m. 
    # Each boolean in T represents whether or not the respective place in the hash table is occupied.
    # R is a list of "suitable" integers j for each bucket B in the original L.
    T = [False] * m
    R = [0]*len(L)

    # We now go through each bucket in decreasing order of length.
    # We find some j >= 0 that is "suitable"
    # j is suitable iff miniHash(m,j) produces a hashfunction that maps all elements in bucket B to
    # unnoccupied spaces in the hashtable without producing clashes within B itself.
    for i in L_sorted_i:
        B = L_sorted[i][0]
        j = 0
        while True:
            # Create hash function and produce hash values for each item in B
            hash_func = miniHash(m,j)
            hash_vals = [hash_func(item) for item in B]
            # Check that each hash table space is unoccupied
            if all([not T[hash_val] for hash_val in hash_vals]):
                # Check that there are no clashes within B itself
                if len(set(hash_vals)) == len(hash_vals):
                    # Mark respective places in T as occupied
                    for l in hash_vals:
                        T[l] = True
                    break
            # Next j value
            j = j+1
        # Mark the hash value for the bucket (using original index)
        R[L_sorted[i][1]] = j

    # Return R
    return R




# Putting it all together:
# compact data structure for representing a perfect hash function

class Hasher:
    def __init__ (self,keys,lam,load):
        # keys : list of keys to be hashed
        # lam  : load on outer table, i.e. average bucket size
        #        (higher lam means more compression but
        #        perfect hash function may be harder to construct)
        # load : desired load on resulting hash table, must be < 1
        # hashEnum : enumeration of hash functions used (e.g. miniHash)
        self.n = len(keys)
        self.r = prevPrime (int(self.n//lam))
        self.m = int(self.n//load)
        HT = buildModHashTable(keys,self.r)
        self.hashChoices = hashCompress(HT,self.m)
        # :results in a very small data structure with no trace of keys!
    def hash (self,key):
        i = modHash (key,self.r)
        h = miniHash (self.m, self.hashChoices[i])
        return h(key)

# We can double-check that our hash function really is perfect
# by building the corresponding ordinary hash table:

def checkPerfectHasher (keys,H):
    T = buildHashTable (keys, lambda key: H.hash(key), H.m)
    clashes = [b for b in T if len(b)>=2]
    if len(clashes)==0:
        print("No clashes!")
        # return T
    else:
        print("Clashes found.")
        return clashes

# FOR INTEREST ONLY:

# Calculating 'essential size' of a Hasher, given a crude compression scheme
# (compression itself not implemented):

import math

def compressedSizeOf(H,bitWidth,maxOutlierSize):
    cutoff = 2 ** bitWidth - 1
    outliers = len([j for j in H.hashChoices if j >= cutoff])
    intermedKeySize = math.ceil(math.log2(H.r))
    return (((H.r - outliers) * bitWidth) +
            (outliers * (maxOutlierSize + intermedKeySize)))

def bestCompression(H):
    maxOutlierSize = math.ceil(math.log2(max(H.hashChoices)))
    comprList = [(i,compressedSizeOf(H,i,maxOutlierSize))
                 for i in range(3,maxOutlierSize)]
    best = comprList[0]
    for i in range(1,len(comprList)):
        if comprList[i][1] < best[1]:
            best = comprList[i]
    return {'bestBitWidth' : best[0],
            'totalBitSize' : best[1],
            'bitsPerKey'   : best[1]/H.n}

# End of file
