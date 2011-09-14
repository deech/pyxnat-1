import os
import zipfile
import sys
import string

from collections import deque, defaultdict

from .schema import class_name
from . import uriutil
from . import schema


def unzip(fzip,
          dest_dir,
          check={'run':lambda z,d: True,'desc':""}):
    """
    Extracts the given zip file to the given directory, but only if all members of the
    archive pass the given check.

        Parameters
        ----------
        src: fzip
            zipfile
        dest_dir: string   
            directory into which to extract the archive
        check: dict
            An dictionary that has the keys:
                 'run' : A function that takes a filename and parent directory and returns Bool. By default
                         this function always returns True.
                 'dest' : A string description of this test. By default this is empty.

        Returns a tuple of type (bool,[string]) where if the extraction ran successfully the first is true and the
        second is a list of files that were extracted, and if not the first is false and the second is the name
        of the failing member.
    """
    for member in fzip.namelist():
        if not check['run'](member,dest_dir):
            return (False,member)

    fzip.extractall(path=dest_dir)
    return (True, map (lambda f: os.path.join(dest_dir,f),fzip.namelist()))

def make_constraints_dict(constraints_str, format):
    return {
        'format':format,
        'constraints' : make_constraints_list(constraints_str)
        }

def make_constraints_list(constraints_str):
    # Make sure the user hasn't asked us to download "ALL" the scans and then asked
    # for them to be constrained to a type.
    constraints_list = []
    for c in constraints_str.split(','):
        cleaned = c.strip()
        if cleaned != "":
            constraints_list.append(cleaned)
            
    constraints_list = list(set(constraints_list))
    if len(constraints_list) > 1 and 'ALL' in constraints_list:
        raise ValueError('The \"ALL\" scan type constraint cannot be used with any other constraint')
    else:
        return constraints_list        
    
def default_zip_name(instance, constraints):
    """
    instance : 'object
             The instance that contains local values needed by this function eg. instance._cbase stores the URI.
    constraints : [str]
             A list of constraints eg. ['T1','T2',...]
             
    Default Zip Name
    ----------------
    Given the project "p", subject "s" and experiment "e", and that the "Scans" (as opposed to "Assessors"
    or "Reconstructions") are being downloaded, and the scan types are constrained to "T1,T2",
    the name of the zip file defaults to:
    "p_s_e_scans_T1_T2.zip"

    Exceptions
    ----------
    ValueError: Raised if the URI associated with this class contains wildcards eg. /projects/proj/subjects/*/experiments/scans
    """
    
    if '%2A' in instance._cbase:
        raise ValueError('URI contains wildcards :' + instance._cbase)
    keys = instance._cbase.split('/')[2::2]
    vals = instance._cbase.split('/')[1::2][1:]
    uri_dict = dict(zip(keys,vals))
    return '_'.join(vals) + '_' + instance._cbase.__class__.__name__ + '_' + '_'.join(constraints)
    
def download (dest_dir, zip_name, uri, instance=None,extract=False, overwrite=False):
    """
    Download all the files at this level that match the given constraint as a zip archive. Should not be called directly
    but from a instance of class that supports bulk downloading eg. "Scans"

        Parameters
        ----------
        dest_dir : string
             directory into which to place the downloaded archive
        zip_name : string
             what to call the saved zip file
        ids_or_constraints : [string]
            A list of constraints or resource ids eg. ['1','2','T1','T2', ...]     
        instance : 'object
             The instance that contains local values needed by this function
        format : string
             The format of resource eg. DICOM/NIFTI ...
        extract: bool
             If True, the files are left unextracted in the parent directory. Default is False             
        overwrite: bool
             If False, check that the file doesn't exist in the parent directory before
             saving it. Default is False.
             
        Exceptions
        ----------
        A generic Exception will be raised if the destination directory is unspecified or the
        
        A LookupError is raised if there are no resources to download.
        An EnvironmentError is raised if any of the following happen:
         - If "overwrite" is False, and
           (a) a zip file with the same name exists in given destination directory or
           (b) extracting the archive overrides an existing file.
         In the second case the downloaded archive is left in the parent directory.

        Return
        ------
        A path to the zip archive if "extract" is False, and a list of extracted files if True.
    """
    if instance is None:
        raise Exception('This function should be called directly but from an instance of a class that supports bulk downloading, eg. "Scans"')
    if dest_dir is None:
        raise Exception('Destination directory is unspecified')
    if zip_name is None:
        raise Exception('Zip file name not specified')

    # Check that there are resources at this level
    available = instance.get(instance)
    # print available
    # if len(available) == 0:
    #     raise LookupError(
    #         'There are no %s to download' % class_name(instance).lower())

    zip_location = os.path.join(dest_dir, zip_name + '.zip')

    print overwrite
    if not overwrite:
        if os.path.exists(zip_location):
            raise EnvironmentError("Unable to download to " + zip_location + " because this file already exists.")

    # Download from the server
    try:
        instance._intf._http.cache.preset(zip_location)
        print uri
        instance._intf._exec(uri)
        # Extract the archive
        fzip = zipfile.ZipFile(zip_location,'r')
        if extract:
            check = {'run': lambda f, d: not os.path.exists(os.path.join(dest_dir,f)),
                     'desc': 'File does not exist in the parent directory'}                                     
            safeUnzip = lambda: unzip(fzip, dest_dir, check) if not overwrite else lambda:unzip(fzip,dest_dir)
            (unzipped, paths) = safeUnzip()()
            if not unzipped:
                fzip.close()
                raise EnvironmentError("Unable to extract " + zip_location + " because file " + paths + " failed the following test: " + check['desc'])
            else:
                return (True, paths)
        else:
            fzip.close()
            return (True,zip_location)
    except EnvironmentError, e:
        print e
        raise e
    except Exception, e:
        print e
        raise e
        
