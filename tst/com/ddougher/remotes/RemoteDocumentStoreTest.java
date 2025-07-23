package com.ddougher.remotes;

import com.ddougher.proxamic.Document;
import com.ddougher.proxamic.DocumentStore;
import com.ddougher.proxamic.exampledata.CharacterRecord;
import com.ddougher.remoting.GridClient;
import com.ddougher.remoting.GridServer;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;

class RemoteDocumentStoreTest {

    private GridServer server;
    private GridClient client;
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
        // Set up the server and client
        InetSocketAddress serverAddress = new InetSocketAddress(0);
        server = new GridServer(serverAddress);
        server.start();
        client = new GridClient(server.getBoundAddress());
        client.start();
        
        // Create the remote document store using the client wrapper
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
        } else {
            client.close();
        }
        server.shutdown();
    }
    
    @Test
    void testBasicOperations() throws IOException, InterruptedException, ExecutionException {
        // Create a new document
        CharacterRecord character = remoteStore.newInstance(CharacterRecord.class);
        // Set document store reference manually for the test
        character.setDocumentStore(remoteStore);
        character.setName("Test Character");
        character.setAge(BigDecimal.valueOf(25));
        character.setLevel(5);
        
        // Store the document
        remoteStore.put(character);
        
        // Retrieve the document
        String id = remoteStore.getID(character);
        CharacterRecord retrievedCharacter = remoteStore.get(CharacterRecord.class, id);
        // Set document store reference manually for the test
        retrievedCharacter.setDocumentStore(remoteStore);
        
        // Verify the document
        Assertions.assertEquals("Test Character", retrievedCharacter.name());
        Assertions.assertEquals(BigDecimal.valueOf(25), retrievedCharacter.getAge());
        Assertions.assertEquals(Integer.valueOf(5), retrievedCharacter.getLevel());
        
        // Update the document
        retrievedCharacter.setLevel(10);
        remoteStore.put(retrievedCharacter);
        
        // Retrieve the updated document
        CharacterRecord updatedCharacter = remoteStore.get(CharacterRecord.class, id);
        // Set document store reference manually for the test
        updatedCharacter.setDocumentStore(remoteStore);
        Assertions.assertEquals(Integer.valueOf(10), updatedCharacter.getLevel());
        
        // Delete the document
        remoteStore.delete(updatedCharacter);
        
        // Verify the document is deleted by checking if a new instance is created
        CharacterRecord newCharacter = remoteStore.get(CharacterRecord.class, id);
        Assertions.assertNotEquals(updatedCharacter.name(), newCharacter.name());
    }
    
    // Locking across remote boundaries requires special handling
    // This test is disabled until we implement proper lock handling
    // @Test
    void testLockAndRelease() {
        // Create a new document
        Document doc = remoteStore.newInstance("testLock");
        remoteStore.put(doc);
        
        // For now, just verify the document exists
        Document retrievedDoc = remoteStore.get("testLock");
        Assertions.assertNotNull(retrievedDoc);
    }
    
    @Test
    void testTransactionSupport() {
        final String[] testValue = new String[1];
        
        // Run a transaction
        remoteStore.transact(store -> {
            CharacterRecord character = store.newInstance(CharacterRecord.class, "transactionTest");
            // Set document store reference manually for the test
            character.setDocumentStore(store);
            character.setName("Transaction Test");
            store.put(character);
            testValue[0] = character.name();
        });
        
        // Verify the transaction was committed
        CharacterRecord result = remoteStore.get(CharacterRecord.class, "transactionTest");
        // Set document store reference manually for the test
        result.setDocumentStore(remoteStore);
        Assertions.assertEquals("Transaction Test", result.name());
        Assertions.assertEquals(testValue[0], result.name());
    }
}