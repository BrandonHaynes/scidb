#!/usr/bin/env python
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#


# This script is used to convert the pre.xml files of user documentation into post.xml files and also generate .test files for harness.
# NOTE: Don't use quotes to specify query.
# The script will only process <programlisting> tags with attribute "language='test'" set.
# Example query statements:
# --afl remove(A)
# --aql create array A <a:int32> [x=0:2,3,0] --show-query=yes --show-output=yes
# --shell --show-output=yes --command=csv2scidb -s 1 -p SNSN &lt; inputFile &gt; outputFile
# In the log file, search for keywords: ScriptException, Invalid, SciDBError  
#   ScriptException : Exception occurred
#   Invalid : Invalid value for one or more options, or Invalid option
#   SciDBError : SciDB query returned an error => Invalid query



import re
import os
import sys
import time
import subprocess
from lxml import etree
from glob import glob



# default values (configurable)
IQUERY_PORT=1239
SHOW_QUERY='yes'
SHOW_OUTPUT='yes'
QUERY_LOGGING='yes'
SHOW_OUTPUT_IN_TEST_FILE='yes'
CWD='.'
CHUNKIFY='no'
ANON='no'
OUTPUT_FORMAT='dcsv'
LOG_FILE='udoc.log'
LOG_LEVEL=3
TEST_DIR_PATH='../../../tests/harness/testcases/t/docscript/'         # path for .test files

validCommands = ["--aql", "--afl", "--shell", "--schema"]

validOptions = ["show-query", "show-output", "command", "query", "output-format", "chunkify", "anon", "show-output-in-test-file", "cwd", "query-logging"]

reservedKeywords = ["SELECT", "INTO", "FROM", "WHERE", "REGRID", "VARIABLE WINDOW", "FIXED WINDOW", "WINDOW", "JOIN", "PARTITION BY", "PRECEDING", "FOLLOWING", "UPDATE", "SET", "AS", "GROUP BY", "AND", "CREATE ARRAY", "LOAD", "SAVE", "CURRENT INSTANCE", "ERRORS", "SHADOW ARRAY", "DROP ARRAY"]

# starting word for two-word keywords
resKeyStart = ["CREATE", "VARIABLE", "FIXED", "PARTITION", "GROUP", "CURRENT", "SHADOW", "DROP"]

replaceables = ['base-path']




# Do Not Change (internal use only)
ERROR_OCCURRED=False
showOutput='yes'
invalidQueryDetected=False
currOutputFormat='auto'


# Determine path for .test files
def set_test_dir(dir_to_search):
  global TEST_DIR_PATH
  mydir = os.path.abspath(dir_to_search)
  mydir = os.path.abspath(mydir+'/'+TEST_DIR_PATH)
  if not os.path.isdir(mydir):
    mydir = os.path.abspath(dir_to_search)
    mydir = mydir + '/../../../tests/harness/testcases/t/docscript'
    mydir = os.path.abspath(mydir)
    if not os.path.isdir(mydir):
      mydir = mydir + '/../../../../../test/testcases/t/docscript'
      mydir = os.path.abspath(mydir)
      if not os.path.isdir(mydir):
        print '\nWarning: Please set valid TEST_DIR_PATH in the script.'
  TEST_DIR_PATH = mydir + '/'


# Logging
def log_it(msg,level):
  if LOG_LEVEL >= level:
    lFile = open(LOG_FILE,'a')
    lFile.write('\n')
    logMsg = '[' + time.asctime() + ']  ' + msg
    lFile.write(logMsg)
    lFile.close()


# Replace XML Markup (new)
def get_xml_markup_new(myText):
  if myText == '':
    return ''
  log_it('myText before replacement:'+myText,4)
  myText = myText.replace('<','&lt;')
  myText = myText.replace('>','&gt;')
  i = 0
  start_n = -1                # start index of token
  end_n = -1                  # end index of token
  newText = ''
  while i < len(myText):
    log_it('checking symbol:'+myText[i],5)
    if myText[i].isalnum():
      if start_n == -1:
        start_n = i
      end_n = i
    else:
      if myText[start_n:end_n+1].upper() in resKeyStart:   # logic for reserved keywords with two words
        log_it('two-word reserved keyword:'+myText[start_n:end_n+1].upper(),4)
        i = i + 2
        while myText[i].isalnum():
          end_n = i
          i = i + 1
      log_it('checking reservedKeyword:'+myText[start_n:end_n+1].upper(),4)
      if myText[start_n:end_n+1].upper() in reservedKeywords:
        log_it('reservedKeyword found',4)
        newText = newText + '<command>'+myText[start_n:end_n+1].upper()+'</command>'
      else:
        log_it('Not a reservedKeyword',4)
        newText = newText + myText[start_n:end_n+1]
      newText = newText + myText[i]
      start_n = -1
      end_n = -1
    i = i + 1
  for kw in replaceables:
    newText = newText.replace(kw,'<replaceable> '+kw+' </replaceable>')
  return newText



# Replace XML Markup
def get_xml_markup(myText):
  global ERROR_OCCURRED
  if myText == '':
    return ''
  myText = myText.replace('<','&lt;')
  myText = myText.replace('>','&gt;')
#  myText = myText.replace('-','&ndash;')
  beginSymbols = ['>', '\(', ',']
  endSymbols = ['<', '\)', ',']
  log_it('myText before replacement:'+myText,5)
  try:
   for kw in reservedKeywords:
    for ele1 in ['\n', ' ']:
      for ele2 in ['\n', ' ']:
        curKW = ele1+kw+ele2
        log_it('curKW='+curKW,5)
        tKW = re.compile(curKW,re.IGNORECASE)
        myText = tKW.sub('<command>' + curKW + '</command>',myText)
        log_it('myText after replacement:'+myText,5)
    log_it('checking for endSymbols',5)
    for ele1 in ['\n', ' ']:
      for ele2 in endSymbols:
        curKW = ele1+kw+ele2
        log_it('curKW='+curKW,5)
        tKW = re.compile(curKW,re.IGNORECASE)
        myText = tKW.sub('<command>' + curKW[:-len(ele2)] + '</command>'+ele2,myText)
        log_it('myText after replacement:'+myText,5)
    log_it('checking for beginSymbols',5)
    for ele2 in ['\n', ' ']:
      for ele1 in beginSymbols:
        curKW = ele1+kw+ele2
        log_it('curKW='+curKW,5)
        tKW = re.compile(curKW,re.IGNORECASE)
        myText = tKW.sub(ele1+'<command>' + curKW[len(ele1):] + '</command>',myText)
        log_it('myText after replacement:'+myText,5)
  except Exception, inst:
    log_it("ScriptException in get_xml_markup",1)
    log_it(inst.message,1)
    ERROR_OCCURRED = True
  myText = myText.replace('\\*','*')
  myText = myText.replace('\\)',')')
  myText = myText.replace('\\(','(')
  for kw in replaceables:
    myText = myText.replace(kw,'<replaceable> '+kw+' </replaceable>')
  log_it('myText after replaceables:'+myText,5)
  return myText


# custom output format for 2D array
def chunkify_it(inp):
  global ERROR_OCCURRED
  opt = ''
  inp = inp.strip()
  if inp.count('[[[[') != 0 or inp[:2] != '[[' or inp[-2:] != ']]':
    log_it('ScriptException: Cannot chunkify',1)
    ERROR_OCCURRED = True
    return inp
  log_it('chunkifying array format',1)
  i = 0
  while(i < len(inp)):
    opt = opt + inp[i]
    if inp[i] == '[' or (inp[i] == ']' and inp[i-1] == ']'):
      opt = opt[:-1] + '\n' + opt[len(opt)-1]
    i = i + 1
  return(opt.strip())


# generate xml block
def generateXML(iqCmdList,pBlock):
  global ERROR_OCCURRED
  pTag = etree.Element("para")
  try:
    for iqCmd in iqCmdList:
      log_it('Current Input is:',3)
      log_it('query: '+iqCmd['query'],3)
      log_it('output: '+iqCmd['output'],3)
#      iqCmd['output'] = get_xml_markup(iqCmd['output'])
      if iqCmd['queryType'] == '--schema':
        if iqCmd['output'] != '':
          res = iqCmd['output']
          arrName = res.split('<')[0].split("'")[1]
          attrClause = res.split('<')[1].split('>')[0].replace(',',',\n')
          dimClause = res.split('<')[1].split('>')[1].split("'")[0].strip()
          sDims = ''
          i = 0
          while ( i < len(dimClause) ):
            sDims += dimClause[i]
            if dimClause[i] == ',' and dimClause[i+1].isalpha():
              sDims += '\n'
            i += 1
          iqCmd['output'] = ''
          if iqCmd['anon'] == 'no':    # Keep arrayName anonymous?
            iqCmd['output'] = '<command>' + arrName + '</command>\n'
          iqCmd['output'] = iqCmd['output'] + '\n&lt; ' + attrClause + ' &gt;\n\n' + sDims
      else:
        iqCmd['output'] = iqCmd['output'].replace('<','&lt;')
        iqCmd['output'] = iqCmd['output'].replace('>','&gt;')
        iqCmd['query'] = iqCmd['query'].replace('<','&lt;')
        iqCmd['query'] = iqCmd['query'].replace('>','&gt;')
      if iqCmd['queryType'] == '--afl' or iqCmd['queryType'] == '--schema':
        iqCmd['query'] = 'AFL% ' + iqCmd['query']
      elif iqCmd['queryType'] == '--aql':
        iqCmd['query'] = get_xml_markup_new('AQL% ' + iqCmd['query'])
      else:
        iqCmd['query'] = '$ ' + iqCmd['query']
      if iqCmd['chunkify'] == 'yes':
        iqCmd['output'] = chunkify_it(iqCmd['output'])
      log_it('After XML Markup:',3)
      log_it('query: '+iqCmd['query'],3)
      log_it('output: '+iqCmd['output'],3)
      log_it('generating xml tag for query',4)
      qTag = etree.fromstring('<programlisting>'+iqCmd['query']+'</programlisting>')
      log_it('generating xml tag for output',4)
      sTag = etree.fromstring('<screen>'+iqCmd['output']+'</screen>')
      if iqCmd['show-query'] == 'yes':
        pTag.append(qTag)
      elif iqCmd['show-query'] == 'no':
        log_it('Skipping display of query as show-query=no.',1)
      else:
        log_it('Invalid value for show-query: '+iqCmd['show-query'],1)
        ERROR_OCCURRED = True
      if iqCmd['show-output'] == 'no':
        log_it('Skipping display of output as show-output=no.',1)
        sTag.text = ''
      elif iqCmd['show-output'] != 'yes':
        log_it('Invalid value for show-output: '+iqCmd['show-output'],1)
        ERROR_OCCURRED = True
      if etree.tostring(sTag).strip() != '<screen/>' and etree.tostring(sTag).strip() != '<screen></screen>':   # add tag only if it is not empty
        pTag.append(sTag)
    tCount = len(pTag.getchildren())
    if pBlock.tail != None and pBlock.tail.strip() != '':  # smartly insert text that is at the end of <programlisting> tag to the last tag available
      if tCount == 0:
        pTag.text = pBlock.tail
      else:
        pTag.getchildren()[tCount-1].tail = pBlock.tail
    for sibling in pBlock.itersiblings():   # add tags that are after <programlisting> tag but before closing of <para> tag
      pTag.append(sibling)
  except Exception, inst:
    log_it("ScriptException in generateXML",1)
    log_it(inst.message,1)
    ERROR_OCCURRED = True
  if etree.tostring(pTag) == '<para/>':
    return None
  return pTag


# Execute shell command (iquery or any other command)
def run_cmd(cmd):
  log_it('Executing: '+cmd,1)
  ret = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
  return ret.stdout.read()


# Resolve cwd for shell command
def resolve_cwd(u_cwd):
  global ERROR_OCCURRED
  n_cwd = u_cwd.replace('"','')
  if n_cwd.find('$BASE_PATH') != -1:
    s_base_path = run_cmd('iquery -p '+str(IQUERY_PORT)+' -odense -aq "project(list(\'instances\'),instance_path)"')
    log_it('Server-instance-path detected: "'+s_base_path+'".',3)
    s_base_path = s_base_path[3:s_base_path.find('/0")')-3]
    log_it('Server-base-path: "'+s_base_path+'".',3)
    n_cwd = n_cwd.replace('$BASE_PATH',s_base_path)
  log_it('cwd specified: "'+n_cwd+'".',3)
  n_cwd = os.path.abspath(n_cwd)
  if os.path.isdir(n_cwd):
    log_it('Changing working directory to "'+n_cwd+'" for shell command.',1)
    return n_cwd
  ERROR_OCCURRED = True
  log_it('Invalid cwd specified: "'+n_cwd+'". Reverting to current working directory.',1)
  return '.'


# Auto-remove arrays and add --cleanup section to .test file
def auto_cleanup(testFileName):
  global ERROR_OCCURRED
  log_it('auto_cleanup arrays',1)
  arr = run_cmd('iquery -p '+str(IQUERY_PORT)+' -odense -aq \'project(sort(list(),uaid),name)\'')
  if arr.lower().find('error description') > -1:
    arr = '[]\n'
  arr_list = arr.split(',')
  list = []
  for ele in arr_list:
    list.append(ele[ele.find("'")+1:ele.rfind("'")])
  iqCmdList = []
  removeStr = ''
  if list[0] != '[]':
    for ele in list:
      res = run_cmd('iquery -p '+str(IQUERY_PORT)+' -aq \'remove('+ele+')\'')
      if res.lower().find('error') > -1:
        print "An error occurred in auto_cleanup section"
        log_it("ScriptException in auto_cleanup",1)
        ERROR_OCCURRED = True
        break
      removeStr = removeStr + '\nremove('+ele+')'
  if testFileName != 'ERROR':
    try:
      testFile = open(testFileName,'a')
      if showOutput == 'no':
        testFile.write('\n--stop-igdata')
      testFile.write('\n\n--stop-query-logging\n')
      testFile.write('\n\n--cleanup\n')
      testFile.write(removeStr+'\n')
      testFile.close()
    except Exception, inst:
      log_it("ScriptException in writing cleanup section to .test file",1)
      log_it(inst.message,1)
      ERROR_OCCURRED = True


# Generate .test file
def generate_test_file(docFile):
  global ERROR_OCCURRED
  try:
    log_it('docFile: '+docFile,3)
    testFileName = TEST_DIR_PATH + os.path.basename(docFile).replace('_pre.xml','.test')
    log_it('testFileName: '+testFileName,3)
    testFile = open(testFileName,'w')
    testFile.write('\n--test')
    testFile.write('\n--start-query-logging\n')
    testFile.close()
    return testFileName
  except Exception, inst:
    log_it("ScriptException in generate_test_file",1)
    log_it(inst.message,1)
    ERROR_OCCURRED = True
  return 'ERROR'


# Add queries to .test file
def add_to_test_file(iqCmdList,fname):
  global ERROR_OCCURRED
  global showOutput
  global currOutputFormat
  queryLoggingDisabled = False

  if fname == 'ERROR':
    return
  try:
    testFile = open(fname,'a')
    for iqCmd in iqCmdList:
      stmt = ''
      if iqCmd['query-logging'] == 'no':
        testFile.write('\n--stop-query-logging')
        queryLoggingDisabled = True
      if iqCmd['queryType'] == '--shell':
        stmt = '--shell'
        if iqCmd['cwd'] != '.':
          stmt = stmt + ' --cwd=' + iqCmd['cwd']
        if iqCmd['show-output'] == 'yes' and iqCmd['show-output-in-test-file'] == 'yes':
          stmt = stmt + ' --store'
        stmt = stmt + ' --command="' + initialize_string(iqCmd['command']) + '"'
      else:
        if iqCmd['output-format'] != currOutputFormat:
          testFile.write('\n--set-format ' + iqCmd['output-format'])
          currOutputFormat = iqCmd['output-format']
          log_it('Setting output format to '+currOutputFormat,3)
        if showOutput != iqCmd['show-output']:
          if iqCmd['show-output'] == 'no':
            testFile.write('\n--start-igdata')
            showOutput = 'no'
            log_it('Changing showOutput to '+iqCmd['show-output'],3)
          elif iqCmd['show-output'] == 'yes':
            testFile.write('\n--stop-igdata')
            showOutput = 'yes'
            log_it('Changing showOutput to '+iqCmd['show-output'],3)
          else:
            log_it('Invalid value for showOutput: '+iqCmd['show-output'],1)
            ERROR_OCCURRED = True
        if iqCmd['show-output-in-test-file'] == 'no':
          if iqCmd['show-output'] != 'no':
            testFile.write('\n--start-igdata')
            showOutput = 'no'
            log_it('Only for test file, Changing showOutput to '+iqCmd['show-output'],3)
        if iqCmd['queryType'] == '--aql':
          stmt = '--aql '
        stmt = stmt + initialize_string(iqCmd['query'])
        stmt = stmt.replace(';',' ')
      testFile.write('\n'+stmt)
      if queryLoggingDisabled == True:
        testFile.write('\n--start-query-logging')
        queryLoggingDisabled = False
    testFile.close()
  except Exception, inst:
    log_it("ScriptException in add_to_test_file",1)
    log_it(inst.message,1)
    ERROR_OCCURRED = True
  return 'ERROR'


# remove newline, extra-spaces, and replace tag symbols.
def initialize_string(myStr):
  myStr = myStr.replace('\n',' ')
  myStr = myStr.replace('&lt;','<')
  myStr = myStr.replace('&gt;','>')
  while myStr.count('  ') > 0:
    myStr = myStr.replace('  ',' ')
  return myStr


# return command type- one from validCommands (afl, aql, shell, schema)
def getCmdType(myStr):
  for cmdType in validCommands:
    if myStr.startswith(cmdType):
      return cmdType
  return "invalid"


# separate commands in the 'programlisting' tag block.
def getCmdList(myStr):
  global ERROR_OCCURRED
  tmpStr = myStr
  commands = []
  try:
    rawList = tmpStr.split("--")
    i=len(rawList)-1 
    while i>=0:
      if getCmdType("--"+rawList[i]) != "invalid":
        commands.append("--"+rawList[i])
      elif i>0:
        rawList[i-1] = rawList[i-1] + " --" + rawList[i]
        rawList[i] = ""
      i=i-1
  except Exception, inst:
    log_it("ScriptException in getCmdList",1)
    log_it(inst.message,1)
    ERROR_OCCURRED = True
  commands.reverse()
  log_it('myStr='+myStr,3)
  log_it('Commands are: '+commands.__str__(),3)
  return commands


# parse command and return a command structure.
def getCmdOptions(cmdStr):
  global ERROR_OCCURRED
  iqCmd = {}
  iqCmd['queryType'] = getCmdType(cmdStr)
  iqCmd['show-output'] = SHOW_OUTPUT
  iqCmd['show-output-in-test-file'] = SHOW_OUTPUT_IN_TEST_FILE
  iqCmd['show-query'] = SHOW_QUERY
  iqCmd['query-logging'] = QUERY_LOGGING
  iqCmd['output-format'] = OUTPUT_FORMAT
  iqCmd['chunkify'] = CHUNKIFY
  iqCmd['anon'] = ANON
  iqCmd['cwd'] = CWD
  tmp = cmdStr.split("--")
  iqCmd['query'] = tmp[1][4:]
  commandExtend = ''
  for ele in tmp:
    if (ele.count("=") == 1) or ele.startswith('command='):
      currOption = ele.split("=",1)
      if currOption[0] in validOptions:
        iqCmd[currOption[0]] = currOption[1]
      elif ele[0:3] not in ['afl', 'aql']:
        log_it('Invalid option detected: ' + currOption[0],1)
        ERROR_OCCURRED = True
    elif (iqCmd['queryType'] == '--shell') and (ele.strip() not in ['', 'shell']):
        commandExtend = commandExtend + ' --' + ele
  if iqCmd['queryType'] == '--shell':
    iqCmd['command'] = iqCmd['command'] + commandExtend
    iqCmd['query'] = iqCmd['command']
  if iqCmd['queryType'] == '--schema':
    iqCmd['query'] = 'show(' + tmp[1].replace('schema','').replace(';','').strip() + ')'
  iqCmd['show-output'] = initialize_string(iqCmd['show-output'].strip())
  iqCmd['show-output-in-test-file'] = initialize_string(iqCmd['show-output-in-test-file'].strip())
  iqCmd['show-query'] = initialize_string(iqCmd['show-query'].strip())
  iqCmd['query-logging'] = initialize_string(iqCmd['query-logging'].strip())
  iqCmd['output-format'] = initialize_string(iqCmd['output-format'].strip())
  iqCmd['chunkify'] = initialize_string(iqCmd['chunkify'].strip())
  iqCmd['anon'] = initialize_string(iqCmd['anon'].strip())
  iqCmd['cwd'] = initialize_string(iqCmd['cwd'].strip())
  log_it('cmdStr: '+cmdStr,3)
  log_it('iqCmd is: '+iqCmd.__str__(),3)
  return iqCmd


def validate_cmd_options(iqCmd):
  res = "ok"
  for ele in iqCmd.keys():
    if ele not in validOptions:
      if ele != 'queryType':
        res = "\n Option: '%s' in Not a valid option" % ele
  return res


def executeCmd(iqCmd):
  global invalidQueryDetected
  execCmd = ''
  if iqCmd['queryType'] == '--shell':
    execCmd = initialize_string(iqCmd['command'])
    if iqCmd['cwd'] != '.':
      execCmd = 'cd ' + resolve_cwd(iqCmd['cwd']) + ' && ' + execCmd
  else:
    execCmd = 'iquery -o%s -p %d -' % (iqCmd['output-format'],IQUERY_PORT)
    if iqCmd['queryType'] == '--afl' or iqCmd['queryType'] == '--schema':
      execCmd = execCmd + 'a'
    execCmd = execCmd + 'q "%s"' % initialize_string(iqCmd['query'])
  res = run_cmd(execCmd)
  if res.lower().find('error description') > 0:
    log_it('SciDBError occurred in executing command "%s": \n %s' % (execCmd,res),1)
    res = ''
    invalidQueryDetected = True
  iqCmd['output'] = res
  return iqCmd



def generatePostBlock(pBlock,tFileName):
  global ERROR_OCCURRED
  tCmds = []
  resCmds = []
  try:
    log_it('pBlock.text is: '+pBlock.text,3)
    log_it('source xml line no.: %d'%pBlock.sourceline,3)
    tCmdList = getCmdList(pBlock.text)
    try:
      for ele in tCmdList:
        tCmds.append(getCmdOptions(ele))
    except Exception, inst:
      log_it('ScriptException in getCmdOptions',1)
      raise Exception(inst)
    validCount = 0
    for ele in tCmds:
      if validate_cmd_options(ele) == 'ok':
        validCount = validCount + 1
    if validCount == len(tCmds):
      for ele in tCmds:
        resCmds.append(executeCmd(ele))
        if ele['query'].strip() == '':
          log_it('Invalid query. Check source xml at line: %d'%pBlock.sourceline,1)
          print "Check source xml at line: ",pBlock.sourceline
          ERROR_OCCURRED = True
      add_to_test_file(resCmds,tFileName)
      return(generateXML(resCmds,pBlock))
  except Exception, inst:
    log_it('ScriptException in generatePostBlock',1)
    log_it('Error in source xml at line: '+str(pBlock.sourceline),1)
    print "Error in source xml at line: ",pBlock.sourceline
    print Exception
    print inst
    ERROR_OCCURRED = True
  return(pBlock)


def process_doc(myDoc):
  global ERROR_OCCURRED
  global showOutput
  global invalidQueryDetected
  global currOutputFormat
  ERROR_OCCURRED = False
  showOutput = 'yes'
  invalidQueryDetected = False
  currOutputFormat = 'auto'
  hasBlock = False
  try:
    parser = etree.XMLParser(ns_clean=False,remove_comments=False,remove_pis=False,strip_cdata=False,resolve_entities=False)
#    print "\nCurrently processing: %s\n "%myDoc
    log_it('Parsing doc: '+myDoc,1)
    doc = etree.parse(myDoc,parser)
    docRoot = doc.getroot()
    preDoc = myDoc
    if myDoc.count('pre') == 0:
      preDoc = myDoc.replace(myDoc[-4:],'_pre'+myDoc[-4:])
    tFileName = generate_test_file(preDoc)
    log_it('Test File Name: '+tFileName,1)
    for pBlock in docRoot.getiterator('programlisting'):
      if pBlock.attrib.has_key('language') and pBlock.attrib['language']=='test':
        hasBlock = True
        rBlock = generatePostBlock(pBlock,tFileName)
        if rBlock == None:
          pBlock.getparent().remove(pBlock)
        else:
          pBlock.getparent().replace(pBlock,rBlock)
    auto_cleanup(tFileName)
    postDoc = preDoc.replace('pre','post')
    log_it('Post Doc name: '+postDoc,1)
    doc.write(postDoc,xml_declaration=True,encoding='UTF-8',pretty_print=True)
    if ERROR_OCCURRED == True:
      print "Error(s) occurred while processing file: " + myDoc
    if invalidQueryDetected == True:
      print "SciDBError: Invalid SciDB queries detected in file: "+ myDoc
    if hasBlock == False:
      print "Warning: No processing blocks found in file: "+myDoc
      log_it("Warning: No processing blocks found in file: "+myDoc,1)
  except Exception, inst:
    print "Exception when processing file: %s" % myDoc
    print inst.message


def process_directory(myDir):
  preFiles = glob(myDir + "/*_pre.xml")
  preFiles.sort()
  print "\nFound %d _pre.xml file(s)." % len(preFiles)
  i = 1
  for preFile in preFiles:
    print "\nCurrently processing (%d/%d): %s " % (i,len(preFiles),preFile)
    process_doc(preFile)
    i = i + 1


if __name__ == "__main__":
  lFile = open(LOG_FILE,'w')
  lFile.close()
  if len(sys.argv) > 1:
    if not os.path.isdir(os.path.abspath(sys.argv[1])):
      print "\nCurrently processing: %s"%sys.argv[1]
      set_test_dir(os.path.dirname(sys.argv[1]))
      process_doc(sys.argv[1])
    else:
      set_test_dir(sys.argv[1])
      process_directory(os.path.abspath(sys.argv[1]))
  else:
    set_test_dir('.')
    process_directory('.')
  print "\ndone."

