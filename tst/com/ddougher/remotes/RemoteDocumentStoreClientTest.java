package com.ddougher.remotes;

import com.ddougher.proxamic.Document;
import com.ddougher.proxamic.DocumentStore;
import com.ddougher.proxamic.DocumentStoreAware;
import com.ddougher.proxamic.exampledata.CharacterRecord;
import com.ddougher.remoting.GridServer;
import org.junit.jupiter.api.*;

import java.io.File;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.file.Files;

/**
 * Tests for the RemoteDocumentStoreClient class
 */
class RemoteDocumentStoreClientTest {

    private GridServer server;
    private DocumentStore remoteStore;
    
    static File tempDir;
    
    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        tempDir = Files.createTempDirectory("mytest").toFile();
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
        
        // Create the remote document store directly
        String storePath = tempDir.getAbsolutePath();
        remoteStore = new RemoteDocumentStoreClient(
            (InetSocketAddress) server.getBoundAddress(),
            storePath
        );
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Close the client (via the close method in RemoteDocumentStoreClient)
        if (remoteStore instanceof RemoteDocumentStoreClient) {
            ((RemoteDocumentStoreClient) remoteStore).close();
        }
        server.shutdown();
    }
    
    @Test
    void testDocumentStoreReferenceIsSet() {
        // Create a new document view that implements DocumentStoreAware
        CharacterRecord character = remoteStore.newInstance(CharacterRecord.class);
        
        // Verify the document store reference is set
        Assertions.assertSame(remoteStore, character.getDocumentStore());
        
        // Store the document
        character.setName("Test Character");
        character.setAge(BigDecimal.valueOf(25));
        character.setLevel(5);
        remoteStore.put(character);
        
        // Retrieve the document and verify the document store reference is set
        String id = remoteStore.getID(character);
        CharacterRecord retrievedCharacter = remoteStore.get(CharacterRecord.class, id);
        Assertions.assertSame(remoteStore, retrievedCharacter.getDocumentStore());
    }
    
    @Test
    void testDocumentStoreReferenceIsSetForDocuments() {
        // Create a new document that implements DocumentStoreAware
        Document doc = remoteStore.newInstance();
        
        // Verify the document store reference is set if it's DocumentStoreAware
        if (doc instanceof DocumentStoreAware) {
            Assertions.assertSame(remoteStore, ((DocumentStoreAware) doc).getDocumentStore());
        }
        
        // Store the document
        remoteStore.put(doc);
        
        // Retrieve the document and verify the document store reference is set
        String id = remoteStore.getID(doc);
        Document retrievedDoc = remoteStore.get(id);
        
        if (retrievedDoc instanceof DocumentStoreAware) {
            Assertions.assertSame(remoteStore, ((DocumentStoreAware) retrievedDoc).getDocumentStore());
        }
    }
    
    @Test
    void testLockSetsDocumentStoreReference() {
        // Create and store a document
        CharacterRecord character = remoteStore.newInstance(CharacterRecord.class, "lockTest");
        character.setName("Lock Test");
        remoteStore.put(character);
        
        // Lock the document and verify the document store reference is set
        CharacterRecord lockedCharacter = remoteStore.lock(CharacterRecord.class, "lockTest");
        Assertions.assertSame(remoteStore, lockedCharacter.getDocumentStore());
        
        // Release the lock
        remoteStore.release(lockedCharacter);
    }
    
    @Test
    void testBasicOperationsWorkWithClient() {
        // Create a new document
        CharacterRecord character = remoteStore.newInstance(CharacterRecord.class);
        character.setName("Client Test");
        character.setAge(BigDecimal.valueOf(30));
        character.setLevel(7);
        
        // Store the document
        remoteStore.put(character);
        
        // Retrieve the document
        String id = remoteStore.getID(character);
        CharacterRecord retrievedCharacter = remoteStore.get(CharacterRecord.class, id);
        
        // Verify the document
        Assertions.assertEquals("Client Test", retrievedCharacter.name());
        Assertions.assertEquals(BigDecimal.valueOf(30), retrievedCharacter.getAge());
        Assertions.assertEquals(Integer.valueOf(7), retrievedCharacter.getLevel());
    }
}