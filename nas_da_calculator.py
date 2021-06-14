#!/usr/bin/python

from __future__ import print_function
from __future__ import division
import rubrik_cdm
import sys
import getopt
import getpass
import urllib3
urllib3.disable_warnings()
import datetime
import pytz

def usage():
    print("Usage goes here!")
    exit(0)

def python_input (message):
    if int(sys.version[0]) > 2:
        in_val = input(message)
    else:
        in_val = raw_input(message)
    return (in_val)

def bytes_to_tb(bytes):
    tb = bytes/1024/1024/1024/1024
    return ("%.1f TB" %(tb))

if __name__ == '__main__':

    DEBUG = False
    latest = True
    user = ""
    password = ""
    outfile = ""
    filesets = []
    token = ""

    optlist, args = getopt.getopt(sys.argv[1:], 'hDc:d:o:t:', ['--help', '--DEBUG', '--creds=', '--date=', '--output=', '--token='])
    for opt, a in optlist:
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-D', '--DEBUG'):
            DEBUG = True
        if opt in ('-c', '--creds'):
            (user, password) = a.split(':')
        if opt in ('-d', '--date'):
            date = datetime.datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
            date_ep = (date - datetime.datetime(1970,1,1)).total_seconds()
            latest = False
        if opt in ('-o', '--output'):
            outfile = a
        if opt in ('-t', 'token'):
            token = a

    try:
        rubrik_node = args[0]
    except:
        usage()
    if not user and not token:
        user = python_input("User: ")
    if not password and not token:
        password = getpass.getpass("Password: ")
    if token != "":
        rubrik = rubrik_cdm.Connect(rubrik_node, api_token=token)
    else:
        rubrik = rubrik_cdm.Connect(rubrik_node, user, password)
    if not latest:
        rubrik_config = rubrik.get('v1', '/cluster/me', timeout=60)
        print ("TZ= " + rubrik_config['timezone']['timezone'])
        rubrik_tz = pytz.timezone(rubrik_config['timezone']['timezone'])
        local_zone = pytz.timezone(str(rubrik_tz))
        utz_zone = pytz.timezone('utc')
        date_tz = rubrik_tz.localize(date)
        date_utc = date_tz.astimezone(pytz.utc).replace(tzinfo=None)
        date_utc = (date_utc - datetime.datetime(1970,1,1)).total_seconds()
        print ("DATE: " + str(date_utc))
    done = False
    offset = 0
    while not done:
        params = {'offset': offset}
        fs_data = rubrik.get('v1', '/fileset', params=params, timeout=60)
        for fs in fs_data['data']:
            try:
                if fs['shareId'] == "":
                    continue
            except:
                continue
            if fs['isPassthrough'] == False:
                continue
            hs_data = rubrik.get('internal', '/host/share/' + str(fs['shareId']), timeout=60)
            fs_inst = {'id': fs['id'], 'host': hs_data['hostname'], 'share': hs_data['exportPoint']}
            filesets.append(fs_inst)
        if not fs_data['hasMore']:
            done = True
        else:
            offset = fs_data['offset']
    total_size = 0
    for fs in filesets:
        print("FS: " + str(fs['share']) + " // " + fs['id'])
        fs_info = rubrik.get('v1', '/fileset/' + str(fs['id']))
        if latest:
            try:
                snap_id = fs_info['snapshots'][-1]['id']
            except IndexError:
                print("No Backups")
                continue
        else:
            snap_date = ""
            snap_id = ""
            for snap in fs_info['snapshots']:
                snap_date = datetime.datetime.strptime(snap['date'][:-5], "%Y-%m-%dT%H:%M:%S")
                print ("\t" + str(snap_date))
                snap_date_ep = (snap_date - datetime.datetime(1970,1,1)).total_seconds()
                print ("\t" + str(snap_date_ep))
                if snap_date_ep >= date_utc:
                    print("SNAP_DATE: = " + str(snap_date))
                    snap_id = snap['id']
                    break
        try:
            snap_info = rubrik.get('v1', '/fileset/snapshot/' + str(snap_id))
        except:
            continue
        fs['size'] = snap_info['size']
        total_size += fs['size']
        print(fs['host'] + ":" + fs['share'] + "," + bytes_to_tb(fs['size']))
    print ("Total:" + "," + bytes_to_tb(total_size))

##TODO Bad logic on date.  Must scan and find the first one that lives at or before the date given