About
=====

This is a script to scrape stuff from `seafile` servers, specifically intended
for the Seafile server of the Rhineland-Palatinate universities
(https://seafile.rlp.net)

Motivation
----------

If you encounter the problem that you cannot download an entire seafile share
because it is too large and there is too many folders to individually get,
this might be the solution for you.
At least that is why I wrote it.

Dependencies
------------

- Reasonably new python (coroutines, fstrings, probably other stuff I have missed, I ran this on 3.8) 
- The `requests` module
- BeautifulSoup

Usage
-----

Basically 

```
scraps.py -o [some_output_dir] [token]
```

The token is the unique part in a share's URI.

See `./scraps.py -h` or the source code for details and other options.

Caveats
-------

- Prone to breakage. As this depends on CSS selectors to find relevant parts of
  a directory listing, this is very prone to breaking should the HTML structure
  of the server change.
- Currently only works on `seafile.rlp.net`. I haven't bothered to make the
  server modifiable. Feel free to submit a pull request.
