import * as React from 'react';
import { styled } from '@mui/material/styles';
import Footer from "../components/footer";
import Layout from '@theme/Layout';
import Card from '@mui/material/Card';
import CardActions from '@mui/material/CardActions';
import CardActionArea from '@mui/material/CardActionArea';
import CardContent from '@mui/material/CardContent';
import CardMedia from '@mui/material/CardMedia';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import Grid from '@mui/material/Unstable_Grid2';
import Collapse from '@mui/material/Collapse';
import IconButton, { IconButtonProps } from '@mui/material/IconButton';
import { Talks, Talk } from "@site/static/talks/talkStrings";
import { Meetups, Meetup } from "@site/static/meetups/meetupStrings";

import AddEvent from '@site/src/components/addevent';

interface ExpandMoreProps extends IconButtonProps {
  isExpand: boolean;
}

const ExpandMore = styled((props: ExpandMoreProps) => {
  const { expand, ...other } = props;
  return <IconButton {...other} />;
})(({ theme }) => ({
  marginX: 'auto',
  transition: theme.transitions.create('transform', {
    duration: theme.transitions.duration.shortest,
  }),
}));

const MeetupCard = ( meetup: Meetup ) => {
	return (
    <Card raised={false} sx={{ width: 640, overflow: "hidden" }}>
    	<CardMedia
        sx={{ width: 640, height: 340, padding: 0 }}
        component="img"
        src={require(`@site/static/img/${meetup.image}`).default}
        title={meetup.description}
      />
	    <CardContent sx={{ width: 640 }}>
	      	<Typography variant="h5" color="text.secondary">
	      		{meetup.city}
	      	</Typography>
	    </CardContent>
	    <CardActions disableSpacing>
          <Button size="small" href={meetup.link}>Join</Button>
        </CardActions>
    </Card>
	)
};

const TalkCard = ( talk: Talk ) => {
  const [expanded, setExpanded] = React.useState(false);

  const handleExpandClick = () => {
    setExpanded(!expanded);
  };

  const DisplayLinks = () => {
    if (talk.video_url) {
      return (
        <CardActions disableSpacing>
          <Button size="small" href={talk.conf_url}>Learn more</Button>
          <Button 
            size="small" 
            href={talk.video_url} 
            sx={{ marginLeft: 2 }}
          >
            Watch
          </Button>
        </CardActions>
      )
    } else {
      return (
        <CardActions disableSpacing>
          <Button size="small" href={talk.conf_url}>Learn more</Button>
        </CardActions>
      )
    }
  };

  return (
    <Card raised={true} sx={{ width: 640 }}>
    <CardActions disableSpacing sx={{ padding: 0 }}>
        <ExpandMore
          expand={expanded}
          orientation="horizontal"
          onClick={handleExpandClick}
          aria-expanded={expanded}
          aria-label="show more"
          sx={{ padding: 0 }}
        >
          <CardActionArea sx={{ padding: 0 }}>
            <CardMedia
              sx={{ width: 640, height: 'auto', padding: 0 }}
              component="img"
              src={require(`@site/static/img/${talk.image}`).default}
              title={talk.conf}
            />
          </CardActionArea>
        </ExpandMore>  
      </CardActions>
      <Collapse in={expanded} orientation="vertical" timeout="auto" unmountOnExit> 
        <CardContent sx={{ width: 640 }}>
          <Typography variant="h4" color="text.primary" className="pb-5">
        		{talk.conf}
        	</Typography>
        	<Typography variant="h5" color="text.secondary">
        		{talk.title}
        	</Typography>
        	{talk.speakers.map(speaker => 
	        	<Typography variant="h6" color="text.secondary">
	        		{speaker}
	        	</Typography>
        	)}
          <Typography variant="body1" color="text.secondary" className="pt-5">
            {talk.description}
          </Typography>
        </CardContent>
        {DisplayLinks()}
      </Collapse>
    </Card>
  )
}

const FillTalksGrid = ( events: Talk[] ) => {
  return (
    <Grid container 
      margin="auto"
      rowSpacing={8}
      columnSpacing={4}
      paddingBottom={4}
      width="85%"
    >
      {events.map(event => 
        <Grid
          marginX="auto"
          justifyItems="top"
        >
       		{TalkCard(event)}
        </Grid >
      )}
    </Grid>
  )
}

const FillMeetupsGrid = ( events: Meetup[] ) => {
  return (
    <Grid container 
      margin="auto"
      rowSpacing={8}
      columnSpacing={4}
      paddingBottom={4}
      width="85%"
    >
      {events.map(event => 
        <Grid
          marginX="auto"
          justifyItems="top"
        > 
          {MeetupCard(event)}
        </Grid >
      )}
    </Grid>
  )
}

export default function DisplayTalks(): JSX.Element {
  const seoTitle = 'Talks';
  const seoDescription = '';

  return (
    <Layout title={seoTitle} description={seoDescription}>

		<div className="title px-4 py-12 text-center lg:py-14 lg:px-0">
		    <h2 className="text-5xl text-color-1">
		        Community Resources
		    </h2>
		</div>

		<div className="text-center">
	    <h2 className="text-4xl text-color-1">
	      Talks
	    </h2>
	  </div>
	  <div className="eco-grid-div">
	    {FillTalksGrid(Talks.sort((a, b) => Date(a.date).localeCompare(Date(b.date))))}
	  </div>

		<div className="text-center">
	    <h2 className="text-4xl text-color-1">
	      Meetup Groups
	    </h2>
	  </div>
	  <div className="eco-grid-div">
	    {FillMeetupsGrid(Meetups.sort((a, b) => Date(a.date).localeCompare(Date(b.date))))}
	  </div>

	  <div className="text-center">
	    <h2 className="text-4xl text-color-1">
	      TSC Meetings
	    </h2>
	  </div>
	  <Grid container 
      margin="auto"
      rowSpacing={8}
      columnSpacing={4}
      paddingBottom={4}
      width="85%"
    >
	    <Grid
        marginX="auto"
        justifyItems="top"
	    > 
			  <Card raised={false} sx={{ height: "auto", width: 840 }}>
			  	<CardMedia
		        sx={{ width: "100%", padding: 0 }}
		        component="img"
		        src={require(`@site/static/img/tsc_screen.png`).default}
		        title="TSC meeting"
		      />
		      <CardContent sx={{ height: 190 }}>
		       	<Typography variant="h5" color="text.secondary">
		       		OpenLineage Technical Steering Committee Meeting (open to all)
		       	</Typography>
		       	<Typography sx={{ my: 1 }} variant="h6" color="text.secondary">
		       		Day/time: every third Wednesday from 9:30am to 10:30am Pacific time
		       	</Typography>
		       	<Typography variant="body1" color="text.secondary">
	            At the monthly meeting, we review recent releases, hear from contributors of major new developments, and feature guest speakers on various topics of interest to the community. Meetings take place on Zoom and are archived on the OpenLineage YouTube Channel. Notes for the meeting are published on the OpenLineage Wiki.
		        </Typography>
		      </CardContent>
		      <CardActions disableSpacing>
          	<Button size="small" href="https://zoom-lfx.platform.linuxfoundation.org/meeting/91792261572?password=7c4c7552-0970-480f-9bdb-0b85257879ac">Zoom</Button>
          	<Button sx={{ marginLeft: 5 }} size="small" href="https://www.youtube.com/@openlineageproject6897/videos">YouTube</Button>
          	<Button sx={{ marginLeft: 5 }} size="small" href="https://wiki.lfaidata.foundation/display/OpenLineage/Monthly+TSC+meeting">Minutes</Button>
        	</CardActions>
			  </Card>
		  </Grid>
	  </Grid>

	  <div className="text-center">
	    <h2 className="text-4xl text-color-1">
	      Contribute
	    </h2>
	  </div>
	  <Grid container 
      margin="auto"
      rowSpacing={8}
      columnSpacing={4}
      paddingBottom={4}
      width="85%"
    >
	    <Grid
        marginX="auto"
        justifyItems="top"
	    >
			<Card raised={false} sx={{ height: "auto", width: 440 }}>
			  	<CardMedia
		        sx={{ width: 440, height: "auto", padding: 5 }}
		        component="img"
		        src={require(`@site/static/img/github.png`).default}
		        title="GitHub"
		      />
		      <CardContent sx={{ height: 145 }}>
		       	<Typography variant="h5" color="text.secondary">
		       		OpenLineage GiHub Organization
		       	</Typography>
		       	<Typography sx={{ my: 1 }} variant="body1" color="text.secondary">
	            Visit GitHub for the main codebase and repos for the website and workshops. Contributions are welcome!
	          </Typography>
		      </CardContent>
		      <CardActions disableSpacing>
          	<Button size="small" href="https://github.com/OpenLineage/OpenLineage/CONTRIBUTING.md">Learn More</Button>
          	<Button sx={{ marginLeft: 5 }}size="small" href="https://github.com/OpenLineage/">GitHub</Button>
          </CardActions>
			  </Card>
	    </Grid>
	    <Grid
        marginX="auto"
        justifyItems="top"
	    >
	    <Card raised={false} sx={{ height: "auto", width: 440 }}>
			  	<CardMedia
		        sx={{ width: 440, height: "auto", padding: 4 }}
		        component="img"
		        src={require(`@site/static/img/slack.png`).default}
		        title="Slack"
		      />
		      <CardContent sx={{ height: 145 }}>
		       	<Typography variant="h5" color="text.secondary">
		       		OpenLineage Slack
		       	</Typography>
		       	<Typography sx={{ my: 1 }} variant="body1" color="text.secondary">
	            Learn about the project, find out about releases and upcoming events, sync up with fellow users and contributors, and get help from OpenLineage experts.
	          </Typography>
		      </CardContent>
		      <CardActions disableSpacing>
          	<Button size="small" href="https://join.slack.com/t/openlineage/shared_invite/zt-2u4oiyz5h-TEmqpP4fVM5eCdOGeIbZvA">Join</Button>
          </CardActions>
			</Card>
			</Grid>
    </Grid>
      
    <div className="bg-bg">
      <Footer />
    </div>
    </Layout>
  )
}
