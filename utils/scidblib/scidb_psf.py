##
## Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006 Python Software Foundation; 
## All Rights Reserved
##
## http://opensource.org/licenses/PythonSoftFoundation.php
##

#
# Notice from SciDB, Inc.
#
# This file contains code we borrowed, that have PSF license.
# To fulfill PSF's License Agreement, we retain the body of the agreement in the file PSF_license.txt,
# and we retain PSF's notice of copyright at the top of this file.
#
# Below is more information on individual code recipe, such as the URL and author name.
#
# - confirm()
#   http://code.activestate.com/recipes/541096/
#   Author: Raghuram Devarakonda
#

def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """
    
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
        
    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False