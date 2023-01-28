from os.path import join, isfile, isdir, exists
from os import mkdir
from shutil import copyfile
from oss2 import logger

DEST_SAVING_PATH = "/root/alist/upload/admin/softwareDB"


def copy_special_file(source_file_path, dest_file_path, main_logger, dest_saving_path=None, file_list=list()) -> bool:
    if main_logger:
        logger = main_logger
    logger.info("starting copy special files")
    logger.debug("source path is: <" + source_file_path + ">\tdestation path is: <" +
                 dest_file_path + ">\tcopying files number is: <" + str(len(file_list)) + ">\n")
    if dest_saving_path:
        if not isdir(dest_saving_path):
            mkdir(dest_saving_path)
    else:
        if not isdir(DEST_SAVING_PATH):
            mkdir(DEST_SAVING_PATH)

    if len(file_list).__eq__(0):
        if (len(source_file_path).__eq__(0) or len(dest_file_path).__eq__(0)):
            logger.info("error source file path or destination file path")
            return False
        if (copyfile(source_file_path, dest_file_path)):
            logger.info("copy file succeed")
            return True
    else:
        if isfile(source_file_path):
            logger.warn(
                "If you want to using a list of files that will be copied, please input a folder path, not a file path when setting source path")
            return False

        if not isfile(dest_file_path):
            if not exists(dest_file_path):
                mkdir(dest_file_path)
        else:
            logger.warn(
                "If you want to using a list of files that will be copied, please input a folder path, not a file path when setting destination path")
            return False
        for file in file_list:
            file_source_path = join(source_file_path, file)
            file_dest_path = join(dest_file_path, file)
            logger.info("current file source path is:" + file_source_path +
                        "\t destation file path is:" + file_dest_path + "\n")
            if copyfile(file_source_path, file_dest_path):
                logger.info("file copy succeed")
            else:
                logger.warn("file copy failed")


if __name__ == "__main__":
    copy_special_file("/home/rane/key",
                      "/home/rane/plugs/key", None)
