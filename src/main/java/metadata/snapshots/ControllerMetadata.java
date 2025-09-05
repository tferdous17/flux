package metadata.snapshots;

import java.util.List;

/**
 * Represents an immutable snapshot of the controller node's metadata
 */
public record ControllerMetadata(String controllerId,
                                 List<String> followerNodeEndpoints,
                                 boolean isActive) {

    public static ControllerMetadata from(proto.ControllerDetails details) {
        return new ControllerMetadata(
                details.getControllerId(),
                details.getFollowerNodeEndpointsList().stream().toList(),
                details.getIsActive()
        );
    }
}
