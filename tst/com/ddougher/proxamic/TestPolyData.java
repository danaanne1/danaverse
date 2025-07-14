package com.ddougher.proxamic;

import com.ddougher.proxamic.DocumentStoreAware;
import com.ddougher.proxamic.DocumentView;
import com.ddougher.proxamic.Getter;

import java.util.List;

public interface TestPolyData extends DocumentView, DocumentStoreAware {


    @Getter("polyData") List<Number []> getPolyData();

}
