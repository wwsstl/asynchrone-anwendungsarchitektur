package de.wwsstl.asynchrone.cloud;

import tools.jackson.databind.JsonNode;

/**
 * Eine Datei aus der {@code inbox}, wie sie an Cloud-API 1 übermittelt wird.
 *
 * @param content Inhalt der Datei, in JSON umgewandelt; er wird als JSON-Wert übermittelt, nicht als Zeichenkette
 */
public record TestdataItem(String fileName, JsonNode content) {
}
