plugins {
    java
}

group = "com.ddougher"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    main {
        java {
            srcDir("src")
        }
        resources {
            srcDir("src")
        }
    }
    test {
        java {
            srcDir("tst")
        }
        resources {
            srcDir("tst")
        }
    }
}

dependencies {
    implementation(project(":storage"))
    // Testing
    testImplementation(project(":"))  // Main project dependency for tests
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

// Set Java compatibility to 1.8
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.withType<Test> {
    useJUnitPlatform()
}