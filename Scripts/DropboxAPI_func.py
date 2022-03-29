import dropbox
from dropbox.dropbox_client import Dropbox

class dropbox_func():
    def __init__(self, appkey, appsecret, accestoken,
                        dbxPath, dataPath):
        self.appkey = appkey
        self.appsecret = appsecret
        self.accestoken = accestoken
        self.dbxPath = dbxPath
        self.dataPath = dataPath
        self.dbx = dropbox.Dropbox(accestoken)

    def files_list(self):
        result = dbx.files_list_folder(dbxPath)     
        print("###### Found ", len(result.entries), " files...")
        return result.entries

    def __download_File(self, file_name):
        self.dbx.files_download_to_file(self.dataPath+file_name,
                                                file.path_lower)
    def __extract_File(self, file_name):
        print("Extracting ",file_name)
        if file_name.endswith('.zip'):
            with ZipFile(self.dataPath + file_name,'r') as zipObj:
                    try:
                        zipObj.extractall(self.dataPath)
                    except Exception as e:
                        print("Error", e)
            print("###### Done downloading and extracting files...")

    def downleadAllfiles(self):
        result = self.files_list()
        for file in result:
            self.__download_File(file.name)
            """self.dbx.files_download_to_file(self.dataPath+file.name,
                                                                                    file.path_lower)"""

        for fileName in os.listdir(self.dataPath):
                    self.__extract_File(fileName)

                                    '''print("Extracting ",fileName)
                                    if fileName.endswith('.zip'):
                                        with ZipFile(self.dataPath + fileName,'r') as zipObj:
                                            try:
                                                zipObj.extractall(self.dataPath)
                                            except Exception as e:
                                                print("Error", e)
                                        print("###### Done downloading and extracting files...")'''

    def downloadFileFromList(self, files_list):
        for file_name in files_list:
            self.__download_File(file_name)
            self.__extract_File(file_name)


