from __future__ import absolute_import

from rest_framework import serializers
from sentry.models import Project, User

from .list import ListField


class NoteSerializer(serializers.Serializer):
    text = serializers.CharField()
    mentions = ListField(required=False)

    def validate_mentions(self, attrs, source):
        if source in attrs and 'group' in self.context:
            mentions = attrs[source]
            project = Project.objects.get(id=self.context['group'].project_id)
        else:
            mentions = None
        if mentions:
            users = User.objects.filter(id__in=mentions)
            for u in users:
                if not project.member_set.filter(user=u).exists():
                    raise serializers.ValidationError('Cannot assign to non-team member')

        return attrs
