plugins {
    kotlin("jvm")
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
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))
    
    // Project dependencies
    implementation(project(":"))  // Main project dependency
    implementation(project(":storage"))
    
    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
}

// Set Java compatibility to 1.8
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

// Set Kotlin JVM target compatibility to match Java
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "1.8"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}