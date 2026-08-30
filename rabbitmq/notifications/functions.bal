# Maps a notification's requested urgency to a numeric message priority. Quorum queues on
# RabbitMQ 4.3+ support strict priorities in the 0-31 range; these levels are spread across
# that range so `urgent` notifications are dispatched ahead of lower-urgency ones.
#
# + urgency - the requested urgency level
# + return - the numeric priority (0-31) corresponding to the urgency
function mapUrgencyToPriority(NotificationUrgency urgency) returns int {
    match urgency {
        URGENCY_LOW => {
            return 0;
        }
        URGENCY_NORMAL => {
            return 8;
        }
        URGENCY_HIGH => {
            return 16;
        }
        URGENCY_URGENT => {
            return 31;
        }
    }
    return 8;
}
