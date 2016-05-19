#!/usr/bin/python
"""Wikipedia Liner:
Given wikipedia dump, puts each article as a single line to be processed by spark afterwards
Usage:
  WikiLiner.py path-to-input-wiki-dump-bz2 path-to-output-wiki-lines-bz2-file
  WikiLiner enwiki-20160501-pages-articles-multistream.xml.bz2 enwiki-20160501-pages-articles-multistream-lines.xml.bz2  
"""

def main():
    import bz2
    import sys
    import os
    incontents = False
    page = ""
    #input = bz2.BZ2File(sys.argv[1] + '', 'r')
    input = file(sys.argv[1] + '', 'r')
    output = bz2.BZ2File(sys.argv[2]+ '', 'w')
    for line in input:
        line = line.strip()
        
        if incontents==True:
            page += "][$#@@#$][" + line
            
        if line=="<page>":
            page = line
            incontents = True # set it now as there are some unneeded metadata
            
        if line=="</page>":
            page += os.linesep
            output.write(page)
            page = ""
            
if __name__ == '__main__':
    main()
