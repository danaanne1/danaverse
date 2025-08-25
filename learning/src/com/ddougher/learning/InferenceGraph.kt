package com.ddougher.learning

import com.ddougher.documentstore.DocumentStoreAware
import com.ddougher.documentstore.DocumentView
import com.ddougher.documentstore.Getter
import com.ddougher.documentstore.Setter

import java.util.List;

// An inference graph is a 100x100 array of MutableList<InferenceRecord<T>>

interface Inference

interface InferenceRecord<T>: DocumentView, DocumentStoreAware {
    @Getter("data") fun getData(): T
    @Setter("data") fun setData(data: T)
    @Getter("weight") fun getWeight(): Float
    @Setter("weight") fun setWeight(weight: Float)
}

interface InferenceItems<T>: DocumentView, DocumentStoreAware {
    @Getter("records") fun records(): List<InferenceRecord<T>>
}

interface InferenceGraph<T>: DocumentView, DocumentStoreAware {
    @Getter("size") fun size(): Int
    @Setter("size") fun size(size: Int)

    @Getter("graph") fun graph(): List<InferenceItems<T>>

    fun append(weights: Array<Float>, data: T ) {
        weights.forEachIndexed { index, weight ->
            graph()[index].records().add((documentStore.newInstance(InferenceRecord::class.java) as InferenceRecord<T>).apply {
                setData(data)
                setWeight(weight)
            })
        }
    }
    fun calculate(influence: List<Float>): kotlin.collections.List<Pair<Float, T>> {
        return influence.flatMapIndexed { index, weight -> graph()[index].records().map { ir -> Pair(ir.getWeight()*weight, ir.getData()) }  }
    }

}




