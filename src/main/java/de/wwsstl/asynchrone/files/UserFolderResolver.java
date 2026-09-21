package de.wwsstl.asynchrone.files;

/**
 * Löst die Ordner eines Benutzers auf. Die Schnittstelle erlaubt später eine Ablösung durch DB-Statusfelder
 * (INBOX, DONE, ERROR), ohne die Aufrufer zu ändern.
 */
public interface UserFolderResolver {

    /**
     * Liefert die Ordner des Benutzers und legt sie beim ersten Zugriff an.
     *
     * @throws InvalidUserIdException wenn die Benutzerkennung nicht als einzelner Ordnername taugt
     */
    UserFolders resolve(String userId);
}
