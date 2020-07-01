#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import os
import datetime
import subprocess
import boto3
from botocore.exceptions import ClientError
import tempfile
import shutil

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

MAX_ARCHIVE_AGE_DAYS = 20


def _write_log(log_file, msg):
    f = open(log_file, 'a')
    f.write('[{}] {}\n'.format(datetime.datetime.today(), msg))
    f.close()


def _to_utf8(s):
   return s.encode('utf-8')


def _to_unicode(s):
    """
    When s is a sequence type it gets converted a string/bytearray
    with elements separated by one space
    """
    if isinstance(s, list) or isinstance(s, tuple):
        s = b" ".join(s)
    if not isinstance(s, str):
        try:
            s = s.decode('utf-8')
        except UnicodeDecodeError as e:
            _write_log(
                "Failed to utf8 decode process output, 'bad' characters will be replaced with U+FFFD. {}.".format(e))
            s = s.decode('utf-8', 'replace')
    return s


def _get_log_tail(log_file):
    if not log_file:
        return ""

    try:
        p = subprocess.Popen("tail -n 100 " + log_file, shell=True, stdout=subprocess.PIPE)
        myStdout, _ = p.communicate()
        myStdout = _to_unicode(myStdout)
        if p.returncode == 0:
            return myStdout.rstrip()
        else:
            return ""
    except:
        return ""


def _format_time_delta(aTimeDelta):
    """Return string representation of time delta as 'x day(s), y hour(s), z min, s sec' for pretty-printing.

    param delta the instance of datetime.timedelta
    """
    mySeconds = aTimeDelta.seconds % 60
    myMinutes = (aTimeDelta.seconds // 60) % 60
    myHours = (aTimeDelta.seconds // 3600) % 24
    myDays = aTimeDelta.days
    if myDays != 0:
        return "{} day(s), {} hour(s), {} min, {} sec".format(myDays, myHours, myMinutes, mySeconds)
    if myHours != 0:
        return "{} hour(s), {} min, {} sec".format(myHours, myMinutes, mySeconds)
    if myMinutes != 0:
        return "{} min, {} sec".format(myMinutes, mySeconds)
    return "{} sec".format(mySeconds)


def _pretty_filesize(filename):
    num = os.path.getsize(filename)
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')


def _svn_backup(svn_dir, backup_dir):
    myCmd = "svnadmin hotcopy --clean-logs {} {}".format(svn_dir, backup_dir)
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=svn_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "svn backup at {} finished with return code {}".format(svn_dir, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "svn backup at {} completed successfully.".format(svn_dir),
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _svn_update(svn_dir):
    myCmd = "svn up --non-interactive --trust-server-cert"
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=svn_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "'svn update' of {} finished with return code {}".format(svn_dir, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "'svn update' of {} completed successfully.".format(svn_dir),
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _get_s3_archive_pwd():
    import pwd
    home_dir = pwd.getpwuid(os.getuid()).pw_dir
    with open("{}/.aws/s3-archive.pwd".format(home_dir)) as f:
        return f.read()


def _is_file_exist_on_s3(file_path, bucket_name):
    s3_client = boto3.client('s3')
    s3_key_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    try:
      obj = s3_client.head_object(Bucket=bucket_name, Key=s3_key_name)
      return obj['ContentLength'] == file_size
    except ClientError as e:
        if int(e.response['Error']['Code']) == 404:
            return False
        else:
            raise


def _upload_to_s3(file_path, bucket_name):
    s3_client = boto3.client('s3')
    s3_key_name = os.path.basename(file_path)
    s3_client.upload_file(file_path, bucket_name, s3_key_name)


def _find_latest_modified_s3_key(bucket_name, key_prefix):
    s3_client = boto3.client('s3')
    try:
        if key_prefix:
            keys = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key_prefix)['Contents']
        else:
            keys = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
        if keys:
            keys.sort(key=lambda k: k['LastModified'])
            return keys[-1]
        else:
            return None
    except boto3.S3.Client.exceptions.NoSuchBucket:
        return None


def _download_from_s3(bucket_name, key_to_download, store_path):
    s3_client = boto3.client('s3')
    s3.download_file(bucket_name, key_to_download, store_path)


def _git_backup(clone_url, repo_archive_path):
    temp_dir = tempfile.mkdtemp()
    myCmd = "git clone --mirror {} ./ && git bundle create {} --all".format(clone_url, repo_archive_path)
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=temp_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    shutil.rmtree(temp_dir, ignore_errors=True)
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "git local backup from {} to {} finished with return code {}.".format(clone_url, repo_archive_path, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "git local backup from {} to {} completed successfully.".format(clone_url, repo_archive_path),
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _mysql_db_backup(db_name, backup_path):
        myCmd = "mysqldump {} > {}".format(db_name, backup_path)
        myProcess = subprocess.Popen(
            myCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        myStdout, myStderr = myProcess.communicate()
        myStdout = _to_unicode(myStdout)
        myStderr = _to_unicode(myStderr)

        if myProcess.returncode != 0:
            return {"ret": False,
                    "description": "MySQL backup of {} to {} finished with return code {}.".format(db_name, backup_path, myProcess.returncode),
                    "stdout": myStdout.rstrip(),
                    "stderr": myStderr.rstrip()}
        return {"ret": True,
                "description": "MySQL backup of {} to {} completed successfully.".format(db_name, backup_path),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}


def _trac_backup(trac_dir, backup_dir):
    myCmd = "/usr/local/bin/trac-admin {} hotcopy {}".format(trac_dir, backup_dir)
    myProcess = subprocess.Popen(
        myCmd, shell=True, cwd=trac_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    myStdout, myStderr = myProcess.communicate()
    myStdout = _to_unicode(myStdout)
    myStderr = _to_unicode(myStderr)

    if myProcess.returncode != 0:
        return {"ret": False,
                "description": "svn backup at {} finished with return code {}.".format(trac_dir, myProcess.returncode),
                "stdout": myStdout.rstrip(),
                "stderr": myStderr.rstrip()}
    return {"ret": True,
            "description": "svn backup at {} completed successfully.".format(trac_dir),
            "stdout": myStdout.rstrip(),
            "stderr": myStderr.rstrip()}


def _archive(src_dir, dest_archive):
    archive_password = _get_s3_archive_pwd()
    myCmd = "7za a -t7z -mhe=on -p{} {} *".format(archive_password, dest_archive)
    if not os.path.exists(os.path.dirname(dest_archive)):
        os.makedirs(os.path.dirname(dest_archive))
    with open(os.devnull, "w") as fnull:
        # skip stdout since it contains all files added to the archive
        myProcess = subprocess.Popen(
            myCmd, shell=True, cwd=src_dir, stdout=fnull, stderr=subprocess.PIPE)
        _, myStderr = myProcess.communicate()
        myStderr = _to_unicode(myStderr)

        if myProcess.returncode != 0:
            return {"ret": False,
                    "description": "archiving {} to {} finished with return code {}.".format(src_dir, dest_archive, myProcess.returncode),
                    "stderr": myStderr.rstrip()}
        return {"ret": True,
                "description": "archiving {} to {} completed successfully.".format(src_dir, dest_archive),
                "stderr": myStderr.rstrip()}


def _cleanup_old_archines(dir, extension, max_age=MAX_ARCHIVE_AGE_DAYS):
    myCmd = r"find %s/*%s -mtime +%d -exec rm -f {} \;" % (dir, extension, max_age)
    with open(os.devnull, "w") as fnull:
        myProcess = subprocess.Popen(
            myCmd, shell=True, cwd=dir, stdout=fnull, stderr=subprocess.PIPE)
        _, myStderr = myProcess.communicate()
        myStderr = _to_unicode(myStderr)

        if myProcess.returncode != 0:
            return {"ret": False,
                    "description": "cleaning up {}/*{} finished with return code {}.".format(dir, extension, myProcess.returncode),
                    "stderr": myStderr.rstrip()}
        return {"ret": True,
                "description": "cleaning up {}/*{} completed successfully.".format(dir, extension),
                "stderr": myStderr.rstrip()}

class SvnBackupType:
    REPO = 1
    WORKING_COPY = 2


def _backup_svn(backup_name_hint, svn_backup_type, svn_url, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] ' + backup_name_hint
    status_detailed = ''
    backup_ok = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    svn_backup_dir = None
    try:
        if svn_backup_type == SvnBackupType.REPO:
            _write_log(log_file, "Backing up svn repo at " + svn_url)
            svn_backup_dir = tempfile.mkdtemp()
            ret = _svn_backup(svn_url, svn_backup_dir)
            svn_url = svn_backup_dir
        elif svn_backup_type == SvnBackupType.WORKING_COPY:
            _write_log(log_file, "Updating " + svn_url)
            ret = _svn_update(svn_url)
        else:
            raise Exception("Unsupported svn backup type {}".format(svn_backup_type))
        _write_log(log_file, '{}\nStdOut: {}\nStdErr: {}\n'.format(
                   ret['description'], ret['stdout'], ret['stderr']))
        if ret['ret']:
            _write_log(log_file, "Archiving {} to {}".format(svn_url, archive_path))
            ret = _archive(svn_url, archive_path)
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
            if ret['ret']:
                if not _is_file_exist_on_s3(archive_path, bucket_name):
                    status_detailed = 'Uploading {} ({}) to S3...'.format(
                        archive_path, _pretty_filesize(archive_path))
                    _upload_to_s3(archive_path, bucket_name)
                    status_detailed += 'done.'
                else:
                    status_detailed = 'The file {} with size {} already exists at to S3, skip upload\n'.format(
                        archive_path, _pretty_filesize(archive_path))
                backup_ok = True

    except Exception as e:
        status_detailed += '\nError: {} {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if svn_backup_dir is not None:
            shutil.rmtree(svn_backup_dir, ignore_errors=True)
        if backup_ok:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to {} finished with status {}'.format(
                   bucket_name, 'SUCCESS' if backup_ok else 'ERROR'))
        return {'retval': backup_ok, 'status_brief': status_brief, 'status_detailed': status_detailed}



#
# Public API
#

def send_email(aSubj, aMsg, aSender, aRecepients, aLogFile, anSmtpSvrHost='localhost', anSmtpSvrPort=25, aUser=None, aPassword=None):
    log_tail = _get_log_tail(aLogFile)
    msg = MIMEMultipart()
    attachment = MIMEText(_to_utf8(log_tail), 'plain', 'utf-8')
    attachment.add_header('Content-Disposition', 'attachment', filename=aLogFile)
    msg.attach(attachment)
    msg.attach(MIMEText(_to_utf8(aMsg), 'plain', 'utf-8'))

    msg['Subject'] = aSubj
    msg['From'] = aSender
    msg['To'] = ', '.join(aRecepients)
    msg['Date'] = formatdate()

    mySmtpSvr = smtplib.SMTP(anSmtpSvrHost, anSmtpSvrPort)
    if aUser and aPassword is not None:
        mySmtpSvr.login(aUser, aPassword)

    # mySmtpSvr.set_debuglevel(1)
    mySmtpSvr.sendmail(aSender, aRecepients, msg.as_string())
    mySmtpSvr.quit()


def backup_dir(hint, dir, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] ' + hint
    status_detailed = ''
    backup_ok = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    try:
        _write_log(log_file, "Archiving {} to {}".format(dir, archive_path))
        ret = _archive(dir, archive_path)
        _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
        if ret['ret']:
            if not _is_file_exist_on_s3(archive_path, bucket_name):
                status_detailed = 'Uploading {} ({}) to S3...'.format(
                    archive_path, _pretty_filesize(archive_path))
                _upload_to_s3(archive_path, bucket_name)
                status_detailed += 'done.'
            else:
                status_detailed = 'The file {} with size {} already exists at to S3, skip upload\n'.format(
                        archive_path, _pretty_filesize(archive_path))
            backup_ok = True
    except Exception as e:
        status_detailed += '\nError: {}. {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if backup_ok:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to {} finished with status {}'.format(
                   bucket_name, 'SUCCESS' if backup_ok else 'ERROR'))
        return {'retval': backup_ok, 'status_brief': status_brief, 'status_detailed': status_detailed}


# Backup LAMP setup including apache HTML directory, apache config directory and MySQL Db
def backup_lamp(backup_name_hint, db_name, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] ' + backup_name_hint
    status_detailed = ''
    backup_ok = False
    start = datetime.datetime.today()
    temp_backup_dir = None
    _write_log(log_file, 'Starting backup')

    try:
        temp_backup_dir = tempfile.mkdtemp()
        shutil.copytree('/var/www/html/', os.path.join(temp_backup_dir, 'var.www.html'))
        shutil.copytree('/etc/apache2/', os.path.join(temp_backup_dir, 'etc.apache2'))
        shutil.copytree('/etc/mysql/', os.path.join(temp_backup_dir, 'etc.mysql'))
        shutil.copytree('/var/log/apache2/', os.path.join(temp_backup_dir, 'var.log.apache2'))
        ret = _mysql_db_backup(db_name, os.path.join(temp_backup_dir, db_name + '.sql'))
        if ret['ret']:
            _write_log(log_file, "Archiving {} to {}".format(temp_backup_dir, archive_path))
            ret = _archive(temp_backup_dir, archive_path)
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))

            if ret['ret']:
                if not _is_file_exist_on_s3(archive_path, bucket_name):
                    status_detailed = 'Uploading {} ({}) to S3...'.format(
                        archive_path, _pretty_filesize(archive_path))
                    _upload_to_s3(archive_path, bucket_name)
                    status_detailed += 'done.'
                else:
                    status_detailed = 'The file {} with size {} already exists at to S3, skip upload\n'.format(
                        archive_path, _pretty_filesize(archive_path))
                backup_ok = True
    except Exception as e:
        status_detailed += '\nError: {}. {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if temp_backup_dir is not None:
            shutil.rmtree(temp_backup_dir, ignore_errors=True)
        if backup_ok:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to {} finished with status {}'.format(
                   bucket_name, 'SUCCESS' if backup_ok else 'ERROR'))
        return {'retval': backup_ok, 'status_brief': status_brief, 'status_detailed': status_detailed}


def backup_svn_repo(backup_name_hint, svn_url, archive_path, bucket_name, log_file):
    return _backup_svn(backup_name_hint, SvnBackupType.REPO, svn_url, archive_path, bucket_name, log_file)


def backup_svn_wc(backup_name_hint, svn_dir, archive_path, bucket_name, log_file):
    return _backup_svn(backup_name_hint, SvnBackupType.WORKING_COPY, svn_dir, archive_path, bucket_name, log_file)


def backup_git_repo(backup_name_hint, clone_url, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] ' + backup_name_hint
    status_detailed = ''
    backup_ok = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    temp_backup_dir = None
    try:
        _write_log(log_file, "Backing up git repo at " + archive_path)
        temp_backup_dir = tempfile.mkdtemp()
        ret = _git_backup(clone_url, temp_backup_dir + '/git_repo.bundle')
        _write_log(log_file, '{}\nStdOut: {}\nStdErr: {}\n'.format(
                   ret['description'], ret['stdout'], ret['stderr']))
        if ret['ret']:
            _write_log(log_file, "Archiving {} to {}".format(temp_backup_dir, archive_path))
            ret = _archive(temp_backup_dir, archive_path)
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
            if ret['ret']:
                if not _is_file_exist_on_s3(archive_path, bucket_name):
                    status_detailed = 'Uploading {} ({}) to S3...'.format(
                        archive_path, _pretty_filesize(archive_path))
                    _upload_to_s3(archive_path, bucket_name)
                    status_detailed += 'done.'
                else:
                    status_detailed = 'The file {} with size {} already exists at to S3, skip upload\n'.format(
                        archive_path, _pretty_filesize(archive_path))
                backup_ok = True
    except Exception as e:
        status_detailed += '\nError: {}. {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if temp_backup_dir is not None:
            shutil.rmtree(temp_backup_dir, ignore_errors=True)
        if backup_ok:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to {} finished with status {}'.format(
                   bucket_name, 'SUCCESS' if backup_ok else 'ERROR'))
        return {'retval': backup_ok, 'status_brief': status_brief, 'status_detailed': status_detailed}


def backup_latest(backup_name_hint, backup_filemask, bucket_name, log_file):
    status_brief = '[S3 Backup] ' + backup_name_hint
    status_detailed = ''
    backup_ok = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup {} to {}'.format(backup_name_hint, bucket_name))
    try:
        files = sorted(glob.glob(backup_filemask), key=lambda filename: os.stat(filename).st_mtime)
        if len(files) == 0:
            backup_ok = True
            status_detailed = 'Nothing to backup in ' + backup_filemask
        else:
            backup_filepath = files[-1]
            if not _is_file_exist_on_s3(backup_filepath, bucket_name):
                status_detailed = 'Uploading {} to S3...'.format(backup_filepath)
                _write_log(log_file, 'Uploading {} ({}) to S3...'.format(
                           backup_filepath, _pretty_filesize(backup_filepath)))
                _upload_to_s3(backup_filepath, bucket_name)
                status_detailed += 'done.'
            else:
                status_detailed = 'The file {} with size {} already exists at to S3, skip upload\n'.format(
                    backup_filepath, _pretty_filesize(archive_path))
            backup_ok = True
    except Exception as e:
        status_detailed += '\nError: {}. {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if backup_ok:
            status_brief += ' OK'
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to {} finished with status {}'.format(
                   bucket_name, 'SUCCESS' if backup_ok else 'ERROR'))
        return {'retval': backup_ok, 'status_brief': status_brief, 'status_detailed': status_detailed}


def backup_trac(backup_name_hint, trac_dir, archive_path, bucket_name, log_file):
    status_brief = '[S3 Backup] ' + backup_name_hint
    status_detailed = ''
    backup_ok = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting backup')
    temp_backup_dir = None
    try:
        _write_log(log_file, "Backing up TRAC at " + trac_dir)
        temp_backup_dir = tempfile.mkdtemp() + "/trac"
        ret = _trac_backup(trac_dir, temp_backup_dir)
        _write_log(log_file, '{}\nStdOut: {}\nStdErr: {}\n'.format(
                   ret['description'], ret['stdout'], ret['stderr']))
        if ret['ret']:
            _write_log(log_file, "Archiving {} to {}".format(temp_backup_dir, archive_path))
            ret = _archive(temp_backup_dir, archive_path)
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
            if ret['ret']:
                if not _is_file_exist_on_s3(archive_path, bucket_name):
                    status_detailed = 'Uploading {} ({}) to S3...'.format(
                        archive_path, _pretty_filesize(archive_path))
                    _upload_to_s3(archive_path, bucket_name)
                    status_detailed += 'done.'
                else:
                    status_detailed = 'The file {} with size {} already exists at to S3, skip upload\n'.format(
                        backup_filepath, _pretty_filesize(archive_path))
                backup_ok = True
    except Exception as e:
        status_detailed += '\nError: {}. {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if temp_backup_dir is not None:
            shutil.rmtree(temp_backup_dir, ignore_errors=True)
        if backup_ok:
            status_brief += ' OK'
            ret = _cleanup_old_archines(dir=os.path.dirname(archive_path),
                                        extension=os.path.splitext(archive_path)[1])
            _write_log(log_file, '{}\nStdErr: {}\n'.format(ret['description'], ret['stderr']))
        else:
            status_brief += ' FAILED'
        end = datetime.datetime.today()
        status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        _write_log(log_file, status_detailed)
        _write_log(log_file, 'Backup to {} finished with status {}'.format(
                   bucket_name, 'SUCCESS' if backup_ok else 'ERROR'))
        return {'retval': backup_ok, 'status_brief': status_brief, 'status_detailed': status_detailed}


def download_latest(bucket_name, file_prefix, store_dir, log_file):
    status_brief = '[S3 Backup] Get the latest backup starting with {} from {}:'.format(
        file_prefix, bucket_name)
    status_detailed = ''
    downloadOk = False
    start = datetime.datetime.today()
    _write_log(log_file, 'Starting download')
    try:
        _write_log(log_file, '[S3 backup] Downloading the latest backup starting with {} from bucket {} to {}'.format(
            file_prefix, bucket_name, store_dir))
        s3_key = _find_latest_modified_s3_key(bucket_name, file_prefix)
        if s3_key:
            store_path = os.path.join(store_dir, s3_key['Key'])
            if not os.path.exists(store_path) or os.path.getsize(store_path) != s3_key['Size']:
                status_detailed = 'Downloading {} from {} to {}...'.format(
                    s3_key['Key'], bucket_name, store_path)
                _download_from_s3(bucket_name, s3_key['Key'], store_path)
            else:
                status_detailed = 'The latest modified file in bucket {} starting with {} already exists locally, skip download\n' % (
                    bucket_name, file_prefix)
        else:
            status_detailed = 'No file found in bucket {} starting with {}, nothing to do\n'.format(
                bucket_name, file_prefix)
        downloadOk = True

        status_detailed += 'done.'
    except Exception as e:
        status_detailed += '\nError: {}. {}'.format(type(e), e)
    except:
        status_detailed += '\nUnknown error'
    finally:
        if downloadOk:
            end = datetime.datetime.today()
            status_brief += ' OK'
            status_detailed += '\nSuccessfully downloaded {} ({})'.format(store_path, _pretty_filesize(store_path))
            status_detailed += '\nElapsed time: ' + _format_time_delta(end - start)
        else:
            status_brief += ' FAILED'
            status_detailed += 'ERROR downloading from {} to {}'.format(bucket_name, store_dir)

        _write_log(log_file, status_detailed)
        return {'retval': downloadOk, 'status_brief': status_brief, 'status_detailed': status_detailed}
