plugins {
    kotlin("jvm") version "1.9.21"
    application
}

group = "com.ddougher"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    main {
        kotlin {
            srcDir("src")
        }
        resources {
            srcDir("src")
        }
    }
    test {
        kotlin {
            srcDir("tst")
        }
        resources {
            srcDir("tst")
        }
    }
}

dependencies {
    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.0")

    // WebSocket
    implementation("org.java-websocket:Java-WebSocket:1.5.3")

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.9.21")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1-Beta")

    // Testing
    testImplementation("org.jetbrains.kotlin:kotlin-test:1.9.21")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<Test> {
    useJUnitPlatform()
}

application {
    mainClass.set("com.ddougher.danaverse.MainKt") // Update this with your actual main class
}
