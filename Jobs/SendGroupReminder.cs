using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Quartz;
using Quartz.Util;
using Rock;
using Rock.Attribute;
using Rock.Communication;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace com.bricksandmortarstudio.ScheduleGroupReminder.Jobs
{
    [GroupField( "Root Group", "The root group" )]
    [IntegerField( "Look Ahead", "The number of days ahead a schedule should be considered for emailing. NOTE: If this job runs multiple times during this interval group members will receive multiple emails" )]
    [SystemEmailField( "Email Template", "The system email that will be used as a template to send to the recipients" )]
    [BooleanField( "Save Communications", "Whether a communication record should be saved for emails sent via this job", Key = "IsSaved" )]
    public class SendGroupReminder : IJob
    {
        public void Execute( IJobExecutionContext context )
        {
            var dataMap = context.JobDetail.JobDataMap;

            bool isSaved = dataMap.GetBooleanValue( "IsSaved" );
            string rootGroupGuid = dataMap.GetString( "RootGroup" );
            string emailTemplateGuid = dataMap.GetString( "EmailTemplate" );
            string lookAhead = dataMap.GetString( "LookAhead" );
            double addDays = Convert.ToDouble( lookAhead.AsInteger() );
            DateTime cutOff = RockDateTime.Now.AddDays( addDays );

            if ( rootGroupGuid.IsNullOrWhiteSpace() || emailTemplateGuid.IsNullOrWhiteSpace() )
            {
                return;
            }
            var rockContext = new RockContext();
            var groupService = new GroupService( rockContext );
            var rootGroup = groupService.Get( rootGroupGuid.AsGuid() );


            // find child groups where the next scheduled date is within the cutoff period
            var descendentGroups = rootGroup.Groups.Where( g => g.Schedule != null && g.Schedule.NextStartDateTime != null && g.Schedule.NextStartDateTime < cutOff || g.GroupLocations != null && g.GroupLocations.Any( gl => gl.Schedules.Any( s => s.NextStartDateTime < cutOff ) ) );

            if ( ( descendentGroups.Any() ) )
            {
                int mailedCount = 0;
                foreach ( var group in descendentGroups )
                {
                    
                    var groupMembers = group.Members
                                            .Where( m =>
                                                 m.GroupMemberStatus == GroupMemberStatus.Active
                                                 && !string.IsNullOrWhiteSpace( m.Person.Email));

                    if ( groupMembers.Any() )
                    {
                        var emailTemplate = new SystemEmailService( rockContext ).Get( dataMap.GetString( "EmailTemplate" ).AsGuid() );
                        var appRoot = Rock.Web.Cache.GlobalAttributesCache.Read().GetValue( "ExternalApplicationRoot" );

                        //Schedule to merge for lava. Pick the group schedule as a default and then the soonest nextstartdatetime otherwise
                        Schedule selectedSchedule = group.Schedule ?? group.GroupLocations.OrderBy(gl => gl.Schedules.First().NextStartDateTime).FirstOrDefault( gl => gl.Schedules.Any() ).Schedules.First();

                        foreach ( var groupMember in groupMembers )
                        {
                            var mergeFields = new Dictionary<string, object>();
                            mergeFields.Add( "Group", group );
                            mergeFields.Add( "GroupMember", groupMember);
                            mergeFields.Add( "Person", groupMember.Person );
                            mergeFields.Add( "Schedule", selectedSchedule );

                            List<string> recipients = new List<string>() { groupMember.Person.Email };

                            Email.Send( emailTemplate.From.ResolveMergeFields( mergeFields ), emailTemplate.FromName.ResolveMergeFields( mergeFields ), emailTemplate.Subject.ResolveMergeFields( mergeFields ), recipients, emailTemplate.Body.ResolveMergeFields( mergeFields ), appRoot, null, null, isSaved );
                            mailedCount++;
                        }
                    }
                }

                context.Result = string.Format( "{0} group members were emailed", mailedCount );
            }

            else
            {
                context.Result = "No group members emailed.";
            }
        }
    }
}
