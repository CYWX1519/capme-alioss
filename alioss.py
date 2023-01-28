from sqlite3 import connect, OperationalError
from oss2 import Auth, Bucket, set_file_logger, set_stream_logger, logger, determine_part_size, SizedFileAdapter
from oss2.models import PutObjectResult, PartInfo
from logging import INFO, DEBUG
from os.path import join, isfile, getmtime, exists, getsize
from os import listdir
from time import sleep, time, localtime, asctime
from random import randint
import uuid
from traceback import print_exc
from datetime import date, timedelta
from plugs.customer_backup import copy_special_file

MAX_RETRIES = 3
COPY_AREA_ONE_NTBAK = "/root/nt"
COPY_AREA_TWO_OSSBAK = "/root/capme-alioss"
DEST_SAVING_PATH = "/root/alist/upload/admin/softwareDB"
COPY_FILES_BUFFER = list()


class AliOSS2:
    def __init__(self,
                 ID,
                 Passwd,
                 end_point="oss-cn-hongkong.aliyuncs.com",
                 # TODO setting your "bucket_name"
                 bucket_name="capme-weboss",
                 file_log="running.log",
                 connect_timeout=30,
                 database_name="records.db",
                 debug_mode=False) -> None:
        assert len(str(ID)) != 0, "Your Login ID Can Not Be Empty!"
        assert len(str(Passwd)) != 0, "Your Login Password Can Not Be Empty!"

        self.ID = ID
        self.Passwd = Passwd
        self.end_point = end_point
        self.connect_timeout = connect_timeout
        self.bucket_name = bucket_name
        self.database_name = database_name
        self.last_modify_time = str()
        if debug_mode:
            set_file_logger(file_log, "oss2", DEBUG)
            set_stream_logger()
        else:
            set_file_logger(file_log, "oss2", INFO)
            self.bucket = Bucket(Auth(self.ID, self.Passwd), self.end_point,
                                 connect_timeout=self.connect_timeout, bucket_name=self.bucket_name)
        self.__init_database()

    def __init_database(self) -> bool:
        client = connect(self.database_name)
        cursor = client.cursor()
        result = cursor.execute(
            "select * from sqlite_master where type='table';")
        table_list = result.fetchall()
        is_create = False
        if len(table_list).__eq__(0):
            is_create = self.__create_database(cursor)
        else:
            is_existed = False
            for table_name in table_list:
                if table_name.__contains__("update_records"):
                    logger.info("table existed!")
                    is_existed = True
                    break
            if not is_existed:
                is_create = self.__create_database(cursor)
        cursor.close()
        client.commit()
        client.close()
        if is_create:
            logger.info("creating database succeed!")
        return is_create

    def __create_database(self, cursor) -> bool:
        try:
            logger.info("creating new table...")
            sql_script = "create table update_records(id integer primary key autoincrement not null,\
                                                      name varchar(255) not null,\
                                                      local_path varchar(512) not null,\
                                                      web_saving_path varchar(512) not null,\
                                                      update_time varchar(255) not null,\
                                                      modified_time varchar(255) not null,\
                                                      update_flag varchar(20) not null,\
                                                      deleted_flag int default 0);"
            logger.debug(sql_script)
            cursor.execute(sql_script)
        except OperationalError:
            logger.error("creating database failed, exiting...")
            exit()
        return True

    def __update_file(self, local_path, web_path) -> None:
        file_list = listdir(local_path)
        for file in file_list:
            file_path = join(local_path, file)
            web_saving_path = join(web_path, file)
            if (file.startswith('.') and not isfile(file_path)) or file.__eq__("temp"):
                continue
            if isfile(file_path):
                logger.debug("file path is: %30s \n\t\t\t\t\t\t       web saving path is: %5s" % (
                    file_path, web_saving_path))
                sql_script = "select modified_time from update_records where name='" + \
                    file + "' and local_path='" + file_path + "';"
                logger.debug(sql_script)
                modify_time = getmtime(file_path)
                try:
                    query_result = self.cursor.execute(sql_script).fetchall()
                    if len(query_result).__eq__(0):
                        retry_count = 0
                        while True:
                            try:
                                retry_count += 1
                                # send_result = self.bucket.put_object_from_file(
                                #     web_saving_path, file_path)
                                send_result = self.__upload_file_in_detial(
                                    file_path, web_saving_path)
                                if send_result.status == 200:
                                    sql_script = "insert into update_records(name,local_path,web_saving_path,update_time,modified_time,update_flag)" + \
                                        " values('" + file + "','" + file_path + "','" + web_saving_path + "','" + str(time()) + "','" + \
                                        str(modify_time) + "','" + \
                                        self.change_flag + "');"
                                    logger.debug(sql_script)
                                    self.cursor.execute(sql_script)
                                    break
                                if retry_count > MAX_RETRIES:
                                    break
                            except Exception:
                                break
                    elif len(query_result).__eq__(1):
                        if str(query_result[0][0]).__eq__(str(modify_time)):
                            sql_script = "update update_records set update_flag='" + \
                                self.change_flag + "' where name='" + file + "';"
                            logger.debug(sql_script)
                            self.cursor.execute(sql_script)
                        else:
                            retry_count = 0
                            while True:
                                try:
                                    retry_count += 1
                                    # send_result = self.bucket.put_object_from_file(
                                    #     web_saving_path, file_path)
                                    send_result = self.__upload_file_in_detial(
                                        file_path, web_saving_path)
                                    if send_result.status == 200:
                                        sql_script = "update update_records set update_time='" + \
                                            str(time()) + "',modified_time='" + \
                                            str(modify_time) + "',update_flag='" + \
                                            self.change_flag + \
                                            "' where name='" + file + "';"
                                        logger.debug(sql_script)
                                        self.cursor.execute(sql_script)
                                        break
                                    if retry_count > MAX_RETRIES:
                                        break
                                except Exception:
                                    break
                    else:
                        raise "more than one file have been recorded!"
                except OperationalError:
                    logger.warn("file<%s> making some warrning: %s" %
                                (file, print_exc()))
                sleep(1)
            else:
                self.__update_file(file_path, web_saving_path)

    def __handle_file(self) -> None:
        with open("file_delete.log", "a+") as f:
            sql_script = "select * from update_records where update_flag!='" + \
                self.change_flag + "' and deleted_flag=0;"
            logger.debug(sql_script)
            query_result_list = self.cursor.execute(sql_script).fetchall()
            if len(query_result_list).__eq__(0):
                return
            for file_list in query_result_list:
                file_name = file_list[1]
                local_path = file_list[2]
                date = asctime(localtime(time()))
                if not exists(local_path):
                    sql_script = "update update_records set deleted_flag=1 where name='" + \
                        file_name + "' and local_path='" + local_path + "';"
                    logger.debug(sql_script)
                    self.cursor.execute(sql_script)
                    log_string = "file_deleted: <%5s, local_location: %10s, web_location: %10s> has been deleted!\n" % (
                        file_name, local_path, file_list[3])
                    logger.warning(log_string)
                    log_string = date + ">>>" + log_string
                    f.write(log_string)
                    # TODO move delete file
                else:
                    # TODO reupload file
                    pass

    def __upload_file_in_detial(self, local_path, web_saving_path) -> PutObjectResult:
        total_file_size = getsize(local_path)
        spilt_threshold = 1024 * 1024 * 10 * 10  # 100M
        if total_file_size.__ge__(spilt_threshold):
            part_size = determine_part_size(
                total_file_size, preferred_size=spilt_threshold)
            upload_id = self.bucket.init_multipart_upload(local_path).upload_id
            parts = []
            with open(local_path, "rb") as f:
                part_number = 1
                offset = 0
                while offset < total_file_size:
                    num_to_upload = min(part_size, total_file_size - offset)
                    result = self.bucket.upload_part(local_path,
                                                     upload_id,
                                                     part_number,
                                                     SizedFileAdapter(f, num_to_upload))
                    parts.append(PartInfo(part_number, result.etag))
                    offset += num_to_upload
                    part_number += 1
            return self.bucket.complete_multipart_upload(local_path, upload_id, parts)
        else:
            return self.bucket.put_object_from_file(web_saving_path, local_path)

    def copy_my_special_files(self) -> bool:
        logger.info("start to backup custom files")

        COPY_FILES_BUFFER.clear()
        COPY_FILES_BUFFER.append("log.txt")
        COPY_FILES_BUFFER.append("savedImages.txt")
        COPY_FILES_BUFFER.append("time.txt")
        copy_special_file(COPY_AREA_ONE_NTBAK, join(
            DEST_SAVING_PATH, "nt"), logger, DEST_SAVING_PATH, COPY_FILES_BUFFER)

        COPY_FILES_BUFFER.clear()
        COPY_FILES_BUFFER.append("key")
        COPY_FILES_BUFFER.append("records.db")
        COPY_FILES_BUFFER.append("running.log")
        COPY_FILES_BUFFER.append("file_delete.log")
        copy_special_file(COPY_AREA_TWO_OSSBAK, join(
            DEST_SAVING_PATH, "weboss"), logger, DEST_SAVING_PATH, COPY_FILES_BUFFER)

    def run(self, local_path, web_root_path, database_file_path) -> None:
        if not isfile(database_file_path) and not database_file_path.endswith("db"):
            logger.error(
                "input database file do not exist or this file is not a database file")
            exit()
        today = date.today()
        tomorrow = today + timedelta(1)
        while True:
            if not str(getmtime(database_file_path)).__eq__(self.last_modify_time):
                try:
                    self.client = connect(self.database_name)
                    self.cursor = self.client.cursor()
                    self.change_flag = str(uuid.uuid1())[0:5]
                    self.__update_file(local_path, web_root_path)
                    self.__handle_file()
                finally:
                    self.cursor.close()
                    self.client.commit()
                    self.client.close()
                    logger.info("closing the database")
                logger.info("sleeping for next changing!")
                self.last_modify_time = str(getmtime(database_file_path))
            else:
                sleep(16)
            today = date.today()
            if today.__eq__(tomorrow):
                sleep(15)
                self.copy_my_special_files()
                self.bucket = Bucket(Auth(self.ID, self.Passwd), self.end_point,
                                     connect_timeout=self.connect_timeout, bucket_name=self.bucket_name)
                tomorrow = today + timedelta(1)


if __name__ == "__main__":
    content = str()
    ID = str()
    passwd = str()
    local_path = str()
    web_root_path = str()
    database_file_path = str()
    if exists("key"):
        with open("key", "r") as f:
            content = f.read()

    if not len(content).__eq__(0):
        content_list = content.split("\n")
        ID = content_list[0]
        passwd = content_list[1]
        local_path = content_list[2]
        web_root_path = content_list[3]
        database_file_path = content_list[4]
    alioss2 = AliOSS2(ID, passwd,
                      debug_mode=False)  # TODO input your ID and Key
    alioss2.run(local_path, web_root_path,
                database_file_path)  # TODO change to your folder, web path, and monitor file
