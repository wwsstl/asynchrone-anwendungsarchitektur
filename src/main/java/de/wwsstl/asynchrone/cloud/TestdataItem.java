package de.wwsstl.asynchrone.cloud;

/** Eine Datei aus der {@code inbox}, wie sie an Cloud-API 1 übermittelt wird. */
public record TestdataItem(String fileName, String content) {
}
