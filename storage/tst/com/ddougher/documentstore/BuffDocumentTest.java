package com.ddougher.documentstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.ddougher.documentstore.exampledata.AbilityScore;
import com.ddougher.documentstore.exampledata.CharacterHistoryRecord;
import com.ddougher.documentstore.exampledata.CharacterRecord;
import com.ddougher.documentstore.exampledata.InventoryItem;

@DisplayName("BuffDocument")
class BuffDocumentTest {

	public static interface Bob extends DocumentView { }

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	<T> T serialize(T record) throws IOException, ClassNotFoundException {
		try (
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream oout = new ObjectOutputStream(bout)) 
		{
			oout.writeObject(record);
			oout.close();
			bout.close();
			try (
					ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
					ObjectInputStream oin = new ObjectInputStream(bin))
			{
				return (T)oin.readObject();
			}
		}
	}

	@Nested
	@DisplayName("assumptions")
	class Assumptions {

		public List<String> getGenericResultMethod() {
			return null;
		}

		@SuppressWarnings("rawtypes")
		public List getGenericResultMethod2() {
			return null;
		}
		
		@Test
		@DisplayName("accessing generic type parameters")
		void generics() throws NoSuchMethodException, SecurityException {
			Type rt = getClass().getMethod("getGenericResultMethod").getGenericReturnType();
			assertTrue(rt instanceof ParameterizedType);
			assertFalse(rt instanceof Class);
			assertTrue(((ParameterizedType)rt).getRawType() instanceof Class);
			assertEquals(String.class, ((ParameterizedType)rt).getActualTypeArguments()[0]);
			Type t2 = getClass().getMethod("getGenericResultMethod2").getGenericReturnType();
			assertTrue(t2 instanceof Class);
		}

		
		@Test
		@DisplayName("dynamic proxies include inherited instances") 
		void proxies1() {
			Bob bob = (Bob) Proxy.newProxyInstance(getClass().getClassLoader(), new Class [] { Bob.class }, new InvocationHandler() {
				
				@Override
				public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
					// TODO Auto-generated method stub
					return null;
				}
			});
			assertTrue(bob instanceof DocumentView);
		}
		
		@Test
		@DisplayName("Puts to asCharBuffer instances dont move position of source buffer")
		void byteOperationAssumption1() {
			String s = new String("This is a string");
			ByteBuffer b = ByteBuffer.allocate(s.length()*2);
			b.asCharBuffer().put(s);
			assertTrue(b.position()==0);
		}
		
	}

	@Nested
	@DisplayName("document")
	class BasicDocument {
		BuffDocument document;
		
		@Test
		@DisplayName("instantiate")
		@BeforeEach
		void init() {
			document = new BuffDocument();
		}
		
		@Test
		@DisplayName("copy instantiate")
		void initFromBytes() {
			document = new BuffDocument(document.toByteBuffer());
		}
		
		@Test
		@DisplayName("serializes with nonserializable docstore")
		void serialization1() throws IOException, ClassNotFoundException {
			document = serialize(document);
		}
		

		@Nested
		@DisplayName("document view")
		class DocumentViews {
			CharacterRecord record;
			
			@Test
			@DisplayName("instantiation") 
			@BeforeEach
			void init () {
				record = document.newInstance(CharacterRecord.class);
			}
			
			@Test
			@DisplayName("serializes with non serializable docstore")
			void serializes() throws IOException, ClassNotFoundException {
				record.name("Dana");
				record = serialize(record);
				assertEquals("Dana",record.name());
			}
			
			@Test
			@DisplayName("primitives and fluent behavior")
			void getAndSetPrimitives() {
				record.setName("Character Name");
				assertEquals("Character Name",record.getName());
				assertEquals("Character Name",record.name());
				assertEquals("Character Name",record.name("Character Name2")); // annotated fluent non builder pattern
				assertEquals(record,record.usingName("Character Name3")); // annotated fluent builder pattern
				assertEquals("Character Name3",record.getName());
				assertEquals("Character Name3",record.name());

				record.setAge(BigDecimal.ONE);
				assertEquals(BigDecimal.ONE, record.getAge());

				record.setLevel(1);
				assertEquals(1, record.getLevel());
				
				assertEquals(record,record.withLevel(2)); // fluent builder pattern
				assertEquals(record,record.setAge(BigDecimal.TEN)); // fluent builder pattern
				
				assertEquals(BigDecimal.TEN, record.getAge());
				assertEquals(2, record.getLevel());
					
			}

			@Disabled
			@Test	
			@DisplayName("ambiguous fluent return")
			void ambiguousFluentReturn() {
			}
			
			@Test
			@DisplayName("unrecognized methods")
			void unrecognizedMethod() {
				assertThrows
				(
						RuntimeException.class,
						() -> record.unrecognizedMethod(), 
						"No path to invoke for unrecognizedMethod"
				);
			}

			@DisplayName("default methods")
			@Test
			void defaultMethod() {
				assertEquals
				(
						"Dana, Level 20 Software Engineer",
						record.usingName("Dana").withLevel(20).characterClass("Software Engineer").sheetHeader()
				);
			}

			@Test
			@DisplayName("equals and hashcode")
			void objectIdentity() {
				CharacterRecord record2 = record.document().as(CharacterRecord.class);
				assertTrue(record2.equals(record));
				assertFalse(record2.equals(null));
				assertFalse(record2.equals(new Object()));
				assertFalse(record2.equals(new Object[0]));
				assertEquals(record2.hashCode(), record.hashCode());
				CharacterRecord record3 = new BuffDocument(document.toByteBuffer()).as(CharacterRecord.class);
				assertNotEquals(record3, record);
				assertNotEquals(record3.hashCode(), record.hashCode());
			}

			private AbilityScore [] getAbilityScores() {
				String [] names = { "Strength", "Intellligence", "Wisdom", "Dexterity", "Constitution", "Charisma" };
				Integer [] values = { 12, 17, 14, 15, 12, 17 };
				AbilityScore [] scores = new AbilityScore[6];
				for (int i = 0; i < scores.length; i++) {
					scores[i] = document.newInstance(AbilityScore.class)
							.withName(names[i])
							.withValue(values[i]);
				}
				return scores;
			}
			
			@Test
			@DisplayName("arrays") 
			void arrays() {
				record.abilityScores(getAbilityScores());
				record = new BuffDocument(record.document().toByteBuffer()).as(CharacterRecord.class);
				AbilityScore [] scoresToCompare = getAbilityScores();
				AbilityScore [] recordsScores = record.abilityScores();
				assertEquals(scoresToCompare.length, recordsScores.length);
				for (int i = 0; i < recordsScores.length; i++) {
					assertEquals(scoresToCompare[i].name(),recordsScores[i].name());
					assertEquals(scoresToCompare[i].value(),recordsScores[i].value());
				}
			}

			@Test
			@DisplayName("typed lists") 
			void lists() {
				AbilityScore [] scores = getAbilityScores();
				assertEquals(0,record.abilityScoreList().size());
				assertTrue(record.abilityScoreList().isEmpty());
				record.abilityScoreList().add(scores[2]);
				assertEquals(1,record.abilityScoreList().size());
				assertFalse(record.abilityScoreList().isEmpty());
				assertEquals("Wisdom",record.abilityScoreList().get(0).name());

				record.abilityScoreList().clear();
				assertEquals(0,record.abilityScoreList().size());
				assertTrue(record.abilityScoreList().isEmpty());
				
				for (AbilityScore score: scores) {
					record.abilityScoreList().add(score);
				}
				
				for (int i = 0; i < scores.length; i++) {
					assertEquals(scores[i], record.abilityScoreList().get(i));
					record.abilityScoreList().set(i, null);
					assertNull(record.abilityScoreList().get(i));
				}

				
			}
			
			@SuppressWarnings("unchecked")
			@Test
			@DisplayName("unchecked lists") 
			void uncheckedLists() {
				AbilityScore [] scores = getAbilityScores();
				assertEquals(0,record.uncheckedAbilityScoreList().size());
				assertTrue(record.uncheckedAbilityScoreList().isEmpty());
				record.uncheckedAbilityScoreList().add(scores[2]);
				assertEquals(1,record.uncheckedAbilityScoreList().size());
				assertFalse(record.uncheckedAbilityScoreList().isEmpty());

				record.uncheckedAbilityScoreList().clear();
				assertEquals(0,record.uncheckedAbilityScoreList().size());
				assertTrue(record.uncheckedAbilityScoreList().isEmpty());
				
			}

			@Test
			@DisplayName("maps") 
			void maps() {
				assertTrue(record.inventoryItems().isEmpty());
				record.inventoryItems().put("head", document.newInstance(InventoryItem.class).withName("Helm of Brilliance"));
				record.inventoryItems().put("belt", document.newInstance(InventoryItem.class).withName("Girdle of Giant Strength"));
				assertEquals(2,record.inventoryItems().size());
				record.inventoryItems().remove("head");
				assertEquals("Girdle of Giant Strength",record.inventoryItems().get("belt").name());
			}

			@Test
			@DisplayName("reference types and serializable primitives") 
			void referenceTypes() {
				Date logDate = new Date();
				record
					.getCharacterHistory()
					.records()
					.add
					(
							document
								.newInstance(CharacterHistoryRecord.class)
								.withLogEntry("Character was created")
								.withLogDate(logDate)
					);
				record = new BuffDocument(record.document().toByteBuffer()).as(CharacterRecord.class);
				assertEquals(1,record.getCharacterHistory().records().size());
				assertEquals("Character was created",record.getCharacterHistory().records().get(0).getLogEntry());
				assertEquals(logDate,record.getCharacterHistory().records().get(0).getLogDate());
			}


		}
	}

	

}
