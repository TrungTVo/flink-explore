package org.quickstart.models;

import org.quickstart.interfaces.CustomFormat;

/**
 * Output record for the "number" key group, holding the running sum of all numeric inputs.
 */
public class CustomNumber implements CustomFormat {
    int result;

    public CustomNumber(int result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "CustomNumber{" + "result=" + result + '}';
    }
}
