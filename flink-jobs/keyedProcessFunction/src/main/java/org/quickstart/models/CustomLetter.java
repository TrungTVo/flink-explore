package org.quickstart.models;

import org.quickstart.interfaces.CustomFormat;

/**
 * Output record for the "letter" key group, holding the concatenated uppercase string of all non-numeric inputs.
 */
public class CustomLetter implements CustomFormat {
    String result;

    public CustomLetter(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "CustomLetter{" + "result=" + result + '}';
    }
}
