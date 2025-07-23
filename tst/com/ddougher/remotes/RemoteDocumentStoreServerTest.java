package com.ddougher.remotes;

import com.ddougher.proxamic.DocumentStore;
import com.ddougher.proxamic.exampledata.CharacterRecord;
import com.ddougher.remoting.GridServer;
import org.junit.jupiter.api.*;

import java.io.File;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.file.Files;

/**
 * Tests for the RemoteDocumentStoreServer
 * This test starts a server and connects a client to verify basic functionality
 */
class RemoteDocumentStoreServerTest {

    private GridServer server;
    private DocumentStore remoteStore;
    
    static File tempDir;
    
    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        tempDir = Files.createTempDirectory("remote_server_test").toFile();
    }
    
    @AfterAll
    static void tearDownAfterClass() throws Exception {
        if (tempDir.exists() && tempDir.isDirectory()) {
            for (File d: tempDir.listFiles())
                d.delete();
            tempDir.delete();
        }
    }
    
    @BeforeEach
    void setUp() throws Exception {
        // Set up the server
        InetSocketAddress serverAddress = new InetSocketAddress(0);
        server = new GridServer(serverAddress);
        server.start();
        
        // Create the remote document store client
        String storePath = tempDir.getAbsolutePath();
        remoteStore = new RemoteDocumentStoreClient(
            (InetSocketAddress) server.getBoundAddress(),
            storePath
        );
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Close the client
        if (remoteStore instanceof RemoteDocumentStoreClient) {
            ((RemoteDocumentStoreClient) remoteStore).close();
        }
        server.shutdown();
    }
    
    @Test
    void testServerBasicFunctionality() {
        // Create a new document
        CharacterRecord character = remoteStore.newInstance(CharacterRecord.class);
        character.setName("Server Test");
        character.setAge(BigDecimal.valueOf(40));
        character.setLevel(10);
        
        // Store the document
        remoteStore.put(character);
        
        // Retrieve the document
        String id = remoteStore.getID(character);
        CharacterRecord retrievedCharacter = remoteStore.get(CharacterRecord.class, id);
        
        // Verify the document
        Assertions.assertEquals("Server Test", retrievedCharacter.name());
        Assertions.assertEquals(BigDecimal.valueOf(40), retrievedCharacter.getAge());
        Assertions.assertEquals(Integer.valueOf(10), retrievedCharacter.getLevel());
        
        // Verify document store reference is set
        Assertions.assertSame(remoteStore, retrievedCharacter.getDocumentStore());
    }
}