package metadata.snapshots;

import java.util.Set;

public record ControllerMetadata(String controllerId,
                                 Set<String> followerNodeEndpoints,
                                 boolean isActive) {
}
