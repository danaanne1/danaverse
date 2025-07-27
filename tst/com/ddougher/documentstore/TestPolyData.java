package com.ddougher.documentstore;

import java.util.List;

public interface TestPolyData extends DocumentView, DocumentStoreAware {


    @Getter("polyData") List<Number []> getPolyData();

}
