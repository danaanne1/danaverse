package com.ddougher.market

import java.io.File
import java.lang.String

class Constants {

    companion object {
        // Existing constants
        val DOC_STORE_BASE_PATH_KEY = "basePath"
        val DOC_STORE_DEFAULT_FOLDER_NAME = System.getProperty("user.home") + File.separator + String.join(File.separator, "Documents", "DanaTrade", "Data")
        val REMOTE_SERVER_ENDPOINT_KEY = "remotes"
        
        // New constants for remote document store
        val REMOTE_STORE_DIRECTORY_KEY = "remoteStoreDirectory"
        val REMOTE_STORE_HOST_KEY = "host"
        val REMOTE_STORE_PORT_KEY = "port"
        val REMOTE_STORE_ENABLED_KEY = "enabled"
        
        // Node names
        val DOC_STORE_NODE = "DocStore"
        val REMOTE_STORE_NODE = "RemoteStore"
    }

}