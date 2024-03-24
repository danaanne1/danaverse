package com.ddougher.market

import java.io.File
import java.lang.String

class Constants {

    companion object {
        val DOC_STORE_BASE_PATH_KEY = "basePath"
        val DOC_STORE_DEFAULT_FOLDER_NAME = System.getProperty("user.home") + File.separator + String.join(File.separator, "Documents", "DanaTrade", "Data")
    }


}