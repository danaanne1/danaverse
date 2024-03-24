package com.ddougher.proxamic;

import com.theunknowablebits.proxamic.DocumentStoreAware;
import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;

import java.util.List;

public interface TestPolyData extends DocumentView, DocumentStoreAware {


    @Getter("polyData") List<Number []> getPolyData();

}
