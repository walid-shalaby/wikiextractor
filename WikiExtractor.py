#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# =============================================================================
#  Version: 2.5 (May 9, 2013)
#  Author: Giuseppe Attardi (attardi@di.unipi.it), University of Pisa
#	   Antonio Fuschetto (fuschett@di.unipi.it), University of Pisa
#
#  Contributors:
#	Leonardo Souza (lsouza@amtera.com.br)
#	Juan Manuel Caicedo (juan@cavorite.com)
#	Humberto Pereira (begini@gmail.com)
#	Siegfried-A. Gevatter (siegfried@gevatter.com)
#	Pedro Assis (pedroh2306@gmail.com)
#	Walid Shalaby (w.fouad.cs@gmail.com)
#
# =============================================================================
#  Copyright (c) 2009. Giuseppe Attardi (attardi@di.unipi.it).
# =============================================================================
#  This file is part of Tanl.
#
#  Tanl is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License, version 3,
#  as published by the Free Software Foundation.
#
#  Tanl is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
# =============================================================================

"""Wikipedia Extractor:
Extracts and cleans text from Wikipedia database dump and stores output in a
number of files of similar size in a given directory.
Each file contains several documents in Tanl document format:
    <doc>
        <title>..</title>
        <docno>..</docno>
        <url>..</url>
        <length>..</length>
        <text>
        ....
        </text
 	</doc>

Usage:
  WikiExtractor.py [options]
  cat enwiki-20150304-pages-articles-multistream.xml | python WikiExtractor.py -g -o wiki20150304-docs -e -a wiki20150304_anchors.txt
  
Options:
  -c, --compress        : compress output files using bzip
  -b, --bytes= n[KM]    : put specified bytes per output file (default 500K)
  -B, --base= URL       : base URL for the Wikipedia pages
  -l, --link            : preserve links
  -n NS, --ns NS        : accepted namespaces (separated by commas)
  -o, --output= dir     : place output files in specified directory (default
                          current)
  -s, --sections	       : preserve sections
  -a, --anchors	       : write anchors for same article in specified file
                          e.g., [[Jaguar|Big Cat]] indicate that article with 
                          title "Jaguar" is also referred to as "Big Cat". 
                          The file will will have lines in below format
                          anchor_text|target_title
  -e, --seealso	       : keep see also titles (usually they represent 
                          semantically related topics)
  -p, --category	       : keep category information
                          e.g., [Category:companies] --> 
                          <Category>companies</Cateogory>
  -h, --help            : display this help and exit
"""

import sys
import time
import gc
import getopt
import urllib
import re
import bz2
import os.path
from html.entities import name2codepoint

### PARAMS ####################################################################

# This is obtained from the dump itself
prefix = None

##
# Whether to preseve links in output
#
keepLinks = False

##
# Whether to flush anchors in output
#
keepAnchors = False
anchors_file = None
anchors_dic = {}

##
# Whether to transform sections into HTML
#
keepSections = False

##
# Whether to keep See also section
#
keepSeeAlso = False

##
# Whether to convert namespaces into xml
#
keepCategoryInfo = False

##
# Recognize only these namespaces
# w: Internal links to the Wikipedia
#
acceptedNamespaces = set(['w'])

# Output XML header for each doc
outputHeader = True

##
# Drop these elements from article text
#
discardElements = set([
        'gallery', 'timeline', 'noinclude', 'pre',
        'table', 'tr', 'td', 'th', 'caption',
        'form', 'input', 'select', 'option', 'textarea',
        'ul', 'li', 'ol', 'dl', 'dt', 'dd', 'menu', 'dir',
        'ref', 'references', 'img', 'imagemap', 'source'
        ])

#=========================================================================
#
# MediaWiki Markup Grammar

# Template = "{{" [ "msg:" | "msgnw:" ] PageName { "|" [ ParameterName "=" AnyText | AnyText ] } "}}" ;
# Extension = "<" ? extension ? ">" AnyText "</" ? extension ? ">" ;
# NoWiki = "<nowiki />" | "<nowiki>" ( InlineText | BlockText ) "</nowiki>" ;
# Parameter = "{{{" ParameterName { Parameter } [ "|" { AnyText | Parameter } ] "}}}" ;
# Comment = "<!--" InlineText "-->" | "<!--" BlockText "//-->" ;
#
# ParameterName = ? uppercase, lowercase, numbers, no spaces, some special chars ? ;
#
#===========================================================================

# Program version
version = '2.5'

##### Main function ###########################################################

def WikiDocument(out, id, title, text):
    url = get_url(id, prefix)
    text = clean(text)

    if outputHeader:
        header = '<doc id="%s" url="%s" title="%s">\n' % (id, url, title)
    # Separate header from text with a newline.
        header += title + '\n'
        header = header.encode('utf-8')
        footer = "\n</doc>"
        if out != sys.stdout:
            out.reserve(len(header) + len(text) + len(footer))
        print(header, end="", file=out)
    else:
        if out != sys.stdout:
            out.reserve(len(text))

    for line in compact(text):
        print(line, end="", file=out)

    if outputHeader:
        print(footer, end="", file=out)

def WikiDocumentTrec(out, id, title, text):
    global anchors_file, anchors_dic
    url = get_url(id, prefix)
    text = clean(text)
    compacted_text = compact(text)

    # calculate text length    
    length = 0
    for line in compacted_text:
        length += len(line)+len(os.linesep)
        
    if outputHeader:
        header = ("<doc>\n<title>%s</title>\n<docno>%s</docno>\n"
                  "<url>%s</url>\n<length>%d</length>\n<text>") % (title, id, url,length)
    # Separate header from text with a newline.
        footer = "</text>\n</doc>"
        if out != sys.stdout:
            out.reserve(len(header) + len(text) + len(footer))
        print(header, end="\n", file=out)
    else:
        if out != sys.stdout:
            out.reserve(len(text))

    for line in compacted_text:
        print(line, end="\n", file=out)

    if outputHeader:
        print(footer, end="\n", file=out)
    
def get_url(id, prefix):
    return "%s?curid=%s" % (prefix, id)

#------------------------------------------------------------------------------

selfClosingTags = [ 'br', 'hr', 'nobr', 'ref', 'references' ]

# handle 'a' separetely, depending on keepLinks
ignoredTags = [
        'b', 'big', 'blockquote', 'center', 'cite', 'div', 'em',
        'font', 'h1', 'h2', 'h3', 'h4', 'hiero', 'i', 'kbd', 'nowiki',
        'p', 'plaintext', 's', 'small', 'span', 'strike', 'strong',
        'sub', 'sup', 'tt', 'u', 'var',
]

placeholder_tags = {'math':'formula', 'code':'codice'}

##
# Normalize title
def normalizeTitle(title):
  # remove leading whitespace and underscores
  title = title.strip(' _')
  # replace sequences of whitespace and underscore chars with a single space
  title = re.compile(r'[\s_]+').sub(' ', title)

  m = re.compile(r'([^:]*):(\s*)(\S(?:.*))').match(title)
  if m:
      prefix = m.group(1)
      if m.group(2):
          optionalWhitespace = ' '
      else:
          optionalWhitespace = ''
      rest = m.group(3)

      ns = prefix.capitalize()
      if ns in acceptedNamespaces:
          # If the prefix designates a known namespace, then it might be
          # followed by optional whitespace that should be removed to get
          # the canonical page name
          # (e.g., "Category:  Births" should become "Category:Births").
          title = ns + ":" + rest.capitalize()
      else:
          # No namespace, just capitalize first letter.
	  # If the part before the colon is not a known namespace, then we must
          # not remove the space after the colon (if any), e.g.,
          # "3001: The_Final_Odyssey" != "3001:The_Final_Odyssey".
          # However, to get the canonical page name we must contract multiple
          # spaces into one, because
          # "3001:   The_Final_Odyssey" != "3001: The_Final_Odyssey".
          title = prefix.capitalize() + ":" + optionalWhitespace + rest
  else:
      # no namespace, just capitalize first letter
      title = title.capitalize();
  return title

##
# Removes HTML or XML character references and entities from a text string.
#
# @param text The HTML (or XML) source text.
# @return The plain text, as a Unicode string, if necessary.

def unescape(text):
    def fixup(m):
        text = m.group(0)
        code = m.group(1)
        try:
            if text[1] == "#":  # character reference
                if text[2] == "x":
                    return unichr(int(code[1:], 16))
                else:
                    return unichr(int(code))
            else:               # named entity
                return unichr(name2codepoint[code])
        except:
            return text # leave as is

    return re.sub("&#?(\w+);", fixup, text)

# Match HTML comments
comment = re.compile(r'<!--.*?-->', re.DOTALL)

# Match elements to ignore
discard_element_patterns = []
for tag in discardElements:
    pattern = re.compile(r'<\s*%s\b[^>]*>.*?<\s*/\s*%s>' % (tag, tag), re.DOTALL | re.IGNORECASE)
    discard_element_patterns.append(pattern)

# Match ignored tags
ignored_tag_patterns = []
def ignoreTag(tag):
    left = re.compile(r'<\s*%s\b[^>]*>' % tag, re.IGNORECASE)
    right = re.compile(r'<\s*/\s*%s>' % tag, re.IGNORECASE)
    ignored_tag_patterns.append((left, right))

for tag in ignoredTags:
    ignoreTag(tag)

# Match selfClosing HTML tags
selfClosing_tag_patterns = []
for tag in selfClosingTags:
    pattern = re.compile(r'<\s*%s\b[^/]*/\s*>' % tag, re.DOTALL | re.IGNORECASE)
    selfClosing_tag_patterns.append(pattern)

# Match HTML placeholder tags
placeholder_tag_patterns = []
for tag, repl in placeholder_tags.items():
    pattern = re.compile(r'<\s*%s(\s*| [^>]+?)>.*?<\s*/\s*%s\s*>' % (tag, tag), re.DOTALL | re.IGNORECASE)
    placeholder_tag_patterns.append((pattern, repl))

# Match preformatted lines
preformatted = re.compile(r'^ .*?$', re.MULTILINE)

# Match external links (space separates second optional parameter)
externalLink = re.compile(r'\[\w+.*? (.*?)\]')
externalLinkNoAnchor = re.compile(r'\[\w+[&\]]*\]')

# Matches bold/italic
bold_italic = re.compile(r"'''''([^']*?)'''''")
bold = re.compile(r"'''(.*?)'''")
italic_quote = re.compile(r"''\"(.*?)\"''")
italic = re.compile(r"''([^']*)''")
quote_quote = re.compile(r'""(.*?)""')

# Matches space
spaces = re.compile(r' {2,}')

# Matches dots
dots = re.compile(r'\.{4,}')

# A matching function for nested expressions, e.g. namespaces and tables.
def dropNested(text, openDelim, closeDelim):
    openRE = re.compile(openDelim)
    closeRE = re.compile(closeDelim)
    # partition text in separate blocks { } { }
    matches = []                # pairs (s, e) for each partition
    nest = 0                    # nesting level
    start = openRE.search(text, 0)
    if not start:
        return text
    end = closeRE.search(text, start.end())
    next = start
    while end:
        next = openRE.search(text, next.end())
        if not next:            # termination
            while nest:         # close all pending
                nest -=1
                end0 = closeRE.search(text, end.end())
                if end0:
                    end = end0
                else:
                    break
            matches.append((start.start(), end.end()))
            break
        while end.end() < next.start():
            # { } {
            if nest:
                nest -= 1
                # try closing more
                last = end.end()
                end = closeRE.search(text, end.end())
                if not end:     # unbalanced
                    if matches:
                        span = (matches[0][0], last)
                    else:
                        span = (start.start(), last)
                    matches = [span]
                    break
            else:
                matches.append((start.start(), end.end()))
                # advance start, find next close
                start = next
                end = closeRE.search(text, next.end())
                break           # { }
        if next != start:
            # { { }
            nest += 1
    # collect text outside partitions
    res = ''
    start = 0
    for s, e in  matches:
        res += text[start:s]
        start = e
    res += text[start:]
    return res

def dropSpans(matches, text):
    """Drop from text the blocks identified in matches"""
    matches.sort()
    res = ''
    start = 0
    for s, e in  matches:
        res += text[start:s]
        start = e
    res += text[start:]
    return res

# Match interwiki links, | separates parameters.
# First parameter is displayed, also trailing concatenated text included
# in display, e.g. s for plural).
#
# Can be nested [[File:..|..[[..]]..|..]], [[Category:...]], etc.
# We first expand inner ones, than remove enclosing ones.
#
wikiLink = re.compile(r'\[\[([^[]*?)(?:\|([^[]*?))?\]\](\w*)')

categoryLink = re.compile(r'\[\[(Category:[^\|\]]*)(\|?.*)\]\]')

seealsoLink = re.compile(r'\[\[([^\|\]]*)(\|?.*)\]\](.*)')

parametrizedLink = re.compile(r'\[\[.*?\]\]')

# Function applied to see also links
def make_seealso_tag(match):

    link = match.group(1)
    return '<seealso>%s</seealso>' % (link)

# Function applied to category links
def make_category_tag(match):

    link = match.group(1)
    colon = link.find(':')
    return '<%s>%s</%s>' % (link[:colon],link[colon+1:],link[:colon])

# Function applied to wikiLinks
def make_anchor_tag(match):
    global keepLinks, anchors_dic, keepAnchors, keepCategoryInfo
    link = match.group(1)
    colon = link.find(':')
    if colon > 0 and link[:colon] not in acceptedNamespaces:
        return ''
    trail = match.group(3)
    anchor = match.group(2)
    if not anchor:
        anchor = link
    elif anchor!=link and keepAnchors: 
        # add anchor to the anchors dictionary if not there
        if anchors_dic.__contains__(link):
            if(anchors_dic[link].__contains__(anchor)==True):
                count = anchors_dic[link][anchor] + 1
            else:
                count = 1
            anchors_dic[link][anchor] = count
        else:
            anchors_dic[link] = {anchor:1}
    anchor += trail
    if keepLinks:
        return '<a href="%s">%s</a>' % (link, anchor)
    else:
        return anchor

def annotateSeeAlso(text):
    new_text = ""
    seealso_text = ""
    inside_seealso = False
    for line in text.split('\n'):
        new_text = new_text + line + "\n"
        if line.startswith("==See also==") or line.startswith("== See also =="):
            inside_seealso = True
        elif inside_seealso==True:
            if not line:
                continue
            elif line.startswith("==") or line.startswith("</page>"): 
                inside_seealso = False
                new_text = new_text + seealso_text + "\n"
            else:
                line = seealsoLink.sub(make_seealso_tag, line)
                if(line.find("<seealso>")!=-1):
                    seealso_text = seealso_text + "\n" + line[line.find("<seealso>"):]

    return new_text
    
def clean(text):
    import html

    # FIXME: templates should be expanded
    # Drop transclusions (template, parser functions)
    # See: http://www.mediawiki.org/wiki/Help:Templates

    global keepCategoryInfo, keepSeeAlso
    
    if keepCategoryInfo==True:
        text = categoryLink.sub(make_category_tag, text)
        
    if keepSeeAlso==True:
        text = annotateSeeAlso(text)
        
    text = dropNested(text, r'{{', r'}}')

    # Drop tables
    text = dropNested(text, r'{\|', r'\|}')

    # Expand links
    text = wikiLink.sub(make_anchor_tag, text)
    # Drop all remaining ones
    text = parametrizedLink.sub('', text)

    # Handle external links
    text = externalLink.sub(r'\1', text)
    text = externalLinkNoAnchor.sub('', text)

    # Handle bold/italic/quote
    text = bold_italic.sub(r'\1', text)
    text = bold.sub(r'\1', text)
    text = italic_quote.sub(r'&quot;\1&quot;', text)
    text = italic.sub(r'&quot;\1&quot;', text)
    text = quote_quote.sub(r'\1', text)
    text = text.replace("'''", '').replace("''", '&quot;')

    ################ Process HTML ###############

    # turn into HTML
    text = unescape(text)
    # do it again (&amp;nbsp;)
    text = unescape(text)

    text = html.unescape(text)

    # Collect spans

    matches = []
    # Drop HTML comments
    for m in comment.finditer(text):
            matches.append((m.start(), m.end()))

    # Drop self-closing tags
    for pattern in selfClosing_tag_patterns:
        for m in pattern.finditer(text):
            matches.append((m.start(), m.end()))

    # Drop ignored tags
    for left, right in ignored_tag_patterns:
        for m in left.finditer(text):
            matches.append((m.start(), m.end()))
        for m in right.finditer(text):
            matches.append((m.start(), m.end()))

    # Bulk remove all spans
    text = dropSpans(matches, text)

    # Cannot use dropSpan on these since they may be nested
    # Drop discarded elements
    for pattern in discard_element_patterns:
        text = pattern.sub('', text)

    # Expand placeholders
    for pattern, placeholder in placeholder_tag_patterns:
        index = 1
        for match in pattern.finditer(text):
            text = text.replace(match.group(), '%s_%d' % (placeholder, index))
            index += 1

    text = text.replace('<<', u'«').replace('>>', u'»')

    #############################################

    # Drop preformatted
    # This can't be done before since it may remove tags
    text = preformatted.sub('', text)

    # Cleanup text
    text = text.replace('\t', ' ')
    text = spaces.sub(' ', text)
    text = dots.sub('...', text)
    text = re.sub(u' (,:\.\)\]»)', r'\1', text)
    text = re.sub(u'(\[\(«) ', r'\1', text)
    text = re.sub(r'\n\W+?\n', '\n', text) # lines with only punctuations
    text = text.replace(',,', ',').replace(',.', '.')
    return text

section = re.compile(r'(==+)\s*(.*?)\s*\1')

def compact(text):
    """Deal with headers, lists, empty sections, residuals of tables"""
    page = []                   # list of paragraph
    headers = {}                # Headers for unfilled sections
    emptySection = False        # empty sections are discarded
    inList = False              # whether opened <UL>
    
    for line in text.split('\n'):

        if not line:
            continue
        # Handle section titles
        m = section.match(line)
        if m:
            title = m.group(2)
            lev = len(m.group(1))
            if keepSections:
                page.append("<h%d>%s</h%d>" % (lev, title, lev))
            if title and title[-1] not in '!?':
                title += '.'
            headers[lev] = title
            # drop previous headers
            to_delete = []
            for i in headers.keys():
                if i > lev:
                    to_delete.append(i)
            for i in to_delete:
                del headers[i]
            emptySection = True            
            continue
        # Handle page title
        if line.startswith('++'):
            title = line[2:-2]
            if title:
                if title[-1] not in '!?':
                    title += '.'
                page.append(title)
        # handle lists
        elif line[0] in '*#:;':
            if keepSections:
                page.append("<li>%s</li>" % line[1:])
            else:
                continue
        # Drop residuals of lists
        elif line[0] in '{|' or line[-1] in '}':
            continue
        # Drop irrelevant lines
        elif (line[0] == '(' and line[-1] == ')') or line.strip('.-') == '':
            continue
        elif len(headers):
            items = headers.items()
            sorted(items)
            for (i, v) in items:
                page.append(v)
            headers.clear()
            page.append(line)   # first line
            emptySection = False
        elif not emptySection:
            page.append(line)

    return page

def handle_unicode(entity):
    numeric_code = int(entity[2:-1])
    if numeric_code >= 0x10000: return ''
    return unichr(numeric_code)

#------------------------------------------------------------------------------

class OutputSplitter:
    def __init__(self, compress, max_file_size, path_name):
        self.dir_index = 0
        self.file_index = -1
        self.compress = compress
        self.max_file_size = max_file_size
        self.path_name = path_name
        self.out_file = self.open_next_file()

    def reserve(self, size):
        cur_file_size = self.out_file.tell()
        if cur_file_size + size > self.max_file_size:
            self.close()
            self.out_file = self.open_next_file()

    def write(self, text):
        self.out_file.write(text)

    def close(self):
        self.out_file.close()

    def open_next_file(self):
        self.file_index += 1
        if self.file_index == 100:
            self.dir_index += 1
            self.file_index = 0
        dir_name = self.dir_name()
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)
        file_name = os.path.join(dir_name, self.file_name())
        if self.compress:
            return bz2.BZ2File(file_name + '.bz2', 'w')
        else:
            return open(file_name, 'w', encoding='utf-8')

    def dir_name(self):
        char1 = self.dir_index % 26
        char2 = self.dir_index / 26 % 26
        return os.path.join(self.path_name, '%c%c' % (ord('A') + char2, ord('A') + char1))

    def file_name(self):
        return 'wiki_%02d' % self.file_index

### READER ###################################################################

tagRE = re.compile(r'(.*?)<(/?\w+)[^>]*>(?:([^<]*)(<.*?>)?)?')

def process_data(input, output):
    import html

    global prefix

    docs = 0
    start = time.time()
    page = []
    id = None
    inText = False
    redirect = False
    for line in input:
        tag = ''
        if '<' in line:
            m = tagRE.search(line)
            if m:
                tag = m.group(2)
        
        line = html.unescape(line)
        if tag == 'page':
            page = []
            redirect = False
        elif tag == 'id' and not id:
            id = m.group(3)
        elif tag == 'title':
            title = m.group(3)
        elif tag == 'redirect':
            redirect = True
            print(m.group(0))
        elif tag == 'text':
            inText = True
            line = line[m.start(3):m.end(3)] + '\n'
            page.append(line)
            if m.lastindex == 4: # open-close
                inText = False
        elif tag == '/text':
            if m.group(1):
                page.append(m.group(1) + '\n')
            inText = False
        elif inText:
            page.append(line)
        elif tag == '/page':
            colon = title.find(':')
            if (colon < 0 or title[:colon] in acceptedNamespaces) and \
                    not redirect:
                print(id, title.encode('utf-8'))
                sys.stdout.flush()
                WikiDocumentTrec(output, id, title, ''.join(page))
                docs+=1
                if not (docs%100000):
                    sys.stderr.write('%d docs in %.2fs, %.2f docs/second\n'%(docs, time.time()-start,docs/(time.time()-start)))
            id = None
            page = []
        elif tag == 'base':
            # discover prefix from the xml dump file
            # /mediawiki/siteinfo/base
            base = m.group(3)
            prefix = base[:base.rfind("/")]

### CL INTERFACE ############################################################

def show_help():
    print >> sys.stdout, __doc__,

def show_usage(script_name):
    print >> sys.stderr, 'Usage: %s [options]' % script_name

##
# Minimum size of output files
minFileSize = 200 * 1024

def main():
    import io
    global keepLinks, keepSections, prefix, acceptedNamespaces, outputHeader
    global keepAnchors, anchors_file, keepSeeAlso, keepCategoryInfo
    script_name = os.path.basename(sys.argv[0])

    try:
        long_opts = ['help', 'compress', 'bytes=', 'basename=', 'links', 'ns=', 'sections', 'category', 'seealso', 'anchors=', 'output=', 'version', 'ignore=', 'text']
        opts, args = getopt.gnu_getopt(sys.argv[1:], 'i:tcb:hln:gea:o:B:sv', long_opts)
    except getopt.GetoptError:
        show_usage(script_name)
        sys.exit(1)

    compress = False
    file_size = 500 * 1024
    output_dir = '.'
    anchors_path = ''

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            show_help()
            sys.exit()
        elif opt in ('-i', '--ignore'):
            ignoreTag(arg)
        elif opt in ('-t', '--text'):
            outputHeader = False
        elif opt in ('-c', '--compress'):
            compress = True
        elif opt in ('-l', '--links'):
            keepLinks = True
        elif opt in ('-s', '--sections'):
            keepSections = True
        elif opt in ('-B', '--base'):
            prefix = arg
        elif opt in ('-b', '--bytes'):
            try:
                if arg[-1] in 'kK':
                    file_size = int(arg[:-1]) * 1024
                elif arg[-1] in 'mM':
                    file_size = int(arg[:-1]) * 1024 * 1024
                else:
                    file_size = int(arg)
                if file_size < minFileSize: raise ValueError()
            except ValueError:
                print >> sys.stderr, \
                '%s: %s: Insufficient or invalid size' % (script_name, arg)
                sys.exit(2)
        elif opt in ('-n', '--ns'):
                acceptedNamespaces = set(arg.split(','))
        elif opt in ('-o', '--output'):
                output_dir = arg
        elif opt in ('-a', '--anchors'):
                anchors_path = arg
                keepAnchors = True
        elif opt in ('-e', '--seealso'):
                keepSeeAlso = True
        elif opt in ('-g', '--category'):
                keepCategoryInfo = True
        elif opt in ('-v', '--version'):
                print('WikiExtractor.py version:', version)
                sys.exit(0)

    if len(args) > 0:
        show_usage(script_name)
        sys.exit(4)

    if output_dir!="-":
        if not os.path.isdir(output_dir):
            try:
                os.makedirs(output_dir)
            except:
                print >> sys.stderr, 'Could not create: ', output_dir
                return

        output_splitter = OutputSplitter(compress, file_size, output_dir)
    else:
        output_splitter = sys.stdout
        if compress:
            raise Exception('Incompatible options, you cannot compress stdout, use a pipe instead')

    if keepAnchors:
        try:
            anchors_file = open(anchors_path,'w',encoding='utf-8')            
        except:
            print >> sys.stderr, 'Could not create: ', anchors_path
            return
        
    if not keepLinks:
        ignoreTag('a')

    
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    process_data(input_stream, output_splitter)
    output_splitter.close()

    # write anchors
    if keepAnchors:
        for link in anchors_dic.keys():
            for anchor,count in anchors_dic[link].items():
                anchors_file.write(link+'|'+anchor+'|'+str(count)+os.linesep)
        anchors_file.close
        
if __name__ == '__main__':
    main()
