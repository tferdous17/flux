package admin;

import commons.header.Properties;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FluxAdminTest {

    @Test
    public void createAdminClientTest() {
        Admin admin = FluxAdmin.create(List.of("localhost:50051"));
    }
}
