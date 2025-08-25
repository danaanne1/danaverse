package com.ddougher.learning

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class LearningModuleTest {
    
    @Test
    fun testModuleConstants() {
        assertEquals("Learning Module", LearningModule.MODULE_NAME)
        assertEquals("0.1.0", LearningModule.VERSION)
    }
}