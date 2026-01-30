import * as React from "react";
import { styled, createTheme } from "@mui/material/styles";
import Footer from "../components/footer";
import Layout from "@theme/Layout";
import Head from "@docusaurus/Head";
import Card from "@mui/material/Card";
import CardActions from "@mui/material/CardActions";
import CardActionArea from "@mui/material/CardActionArea";
import CardContent from "@mui/material/CardContent";
import CardMedia from "@mui/material/CardMedia";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Unstable_Grid2";
import Collapse from "@mui/material/Collapse";
import IconButton, { IconButtonProps } from "@mui/material/IconButton";
import { Talks, Talk } from "@site/static/talks/talkStrings";
import { Meetups, Meetup } from "@site/static/meetups/meetupStrings";

const theme = createTheme({
  breakpoints: {
    values: {
      sm: 600,
    },
  },
});

interface ExpandMoreProps extends IconButtonProps {
  isExpand: boolean;
}

const ExpandMore = styled((props: ExpandMoreProps) => {
  const { isExpand, ...other } = props;
  return <IconButton {...other} />;
})(({ theme }) => ({
  marginX: "auto",
  transition: theme.transitions.create("transform", {
    duration: theme.transitions.duration.shortest,
  }),
}));

const MeetupCard = (meetup: Meetup) => {
  let imgPath = "/img/" + meetup.image;
  return (
    <Card
      raised={false}
      sx={{
        width: 640,
        overflow: "hidden",
        [theme.breakpoints.down("sm")]: {
          width: 300,
          overflow: "hidden",
        },
      }}
    >
      <CardMedia
        sx={{
          width: 640,
          height: 340,
          padding: 0,
          [theme.breakpoints.down("sm")]: {
            width: 300,
            height: "auto",
            padding: 0,
          },
        }}
        component="img"
        src={imgPath}
        title={meetup.city}
      />
      <CardContent
        sx={{
          width: 640,
          [theme.breakpoints.down("sm")]: {
            width: 300,
          },
        }}
      >
        <Typography variant="h5" color="text.secondary">
          {meetup.city}
        </Typography>
      </CardContent>
      <CardActions disableSpacing>
        <Button size="small" href={meetup.link}>
          Join
        </Button>
      </CardActions>
    </Card>
  );
};

const TalkCard = (talk: Talk) => {
  const [expanded, setExpanded] = React.useState(false);

  const handleExpandClick = () => {
    setExpanded(!expanded);
  };

  const DisplayLinks = () => {
    if (talk.video_url) {
      return (
        <CardActions disableSpacing>
          <Button size="small" href={talk.conf_url}>
            Learn more
          </Button>
          <Button size="small" href={talk.video_url} sx={{ marginLeft: 2 }}>
            Watch
          </Button>
        </CardActions>
      );
    } else {
      return (
        <CardActions disableSpacing>
          <Button size="small" href={talk.conf_url}>
            Learn more
          </Button>
        </CardActions>
      );
    }
  };

  let imgPath = "/img/" + talk.image;
  return (
    <Card
      raised={true}
      sx={{
        width: 640,
        [theme.breakpoints.down("sm")]: {
          width: 300,
        },
      }}
    >
      <CardActions disableSpacing sx={{ padding: 0 }}>
        <ExpandMore
          isExpand={expanded}
          onClick={handleExpandClick}
          aria-expanded={expanded}
          aria-label="show more"
          sx={{ padding: 0 }}
        >
          <CardActionArea sx={{ padding: 0 }}>
            <CardMedia
              sx={{
                width: 640,
                height: "auto",
                padding: 0,
                [theme.breakpoints.down("sm")]: {
                  width: 300,
                  height: "auto",
                },
              }}
              component="img"
              src={imgPath}
              title={talk.conf}
            />
          </CardActionArea>
        </ExpandMore>
      </CardActions>
      <Collapse in={expanded} orientation="vertical" timeout="auto" unmountOnExit>
        <CardContent
          sx={{
            width: 640,
            [theme.breakpoints.down("sm")]: {
              width: 300,
              height: "auto",
            },
          }}
        >
          <Typography variant="h4" color="text.primary" className="pb-5">
            {talk.conf}
          </Typography>
          <Typography variant="h5" color="text.secondary">
            {talk.title}
          </Typography>
          {talk.speakers.map((speaker) => (
            <Typography variant="h6" color="text.secondary" key={speaker.toString()}>
              {speaker}
            </Typography>
          ))}
          <Typography variant="body1" color="text.secondary" className="pt-5">
            {talk.description}
          </Typography>
        </CardContent>
        {DisplayLinks()}
      </Collapse>
    </Card>
  );
};

const FillTalksGrid = (events: Talk[]) => {
  return (
    <Grid container margin="auto" rowSpacing={8} columnSpacing={4} paddingBottom={4} width="85%">
      {events.map((event) => (
        <Grid marginX="auto" justifyItems="top" key={event.toString()}>
          {TalkCard(event)}
        </Grid>
      ))}
    </Grid>
  );
};

const FillMeetupsGrid = (events: Meetup[]) => {
  return (
    <Grid container margin="auto" rowSpacing={8} columnSpacing={4} paddingBottom={4} width="85%">
      {events.map((event) => (
        <Grid marginX="auto" justifyItems="top" key={event.toString()}>
          {MeetupCard(event)}
        </Grid>
      ))}
    </Grid>
  );
};

export default function CommunityResources(): JSX.Element {
  const seoTitle = "Community";
  const seoDescription =
    "Learn about community resources available from OpenLineage including recorded talks, meetup groups, and ways to contribute.";

  return (
    <Layout title={seoTitle} description={seoDescription}>
      <Head>
        <meta property="og:image" content="https://openlineage.io/img/community-thumb.png" />
        <meta property="twitter:image" content="https://openlineage.io/img/community-thumb.png" />
      </Head>
      <div className="title px-4 py-12 text-center lg:py-14 lg:px-0">
        <h2 className="text-5xl text-color-1">Community Resources</h2>
      </div>

      <div className="text-center">
        <h2 className="text-4xl text-color-1">Get Involved</h2>
      </div>
      <Grid
        container
        margin="auto"
        rowSpacing={4}
        columnSpacing={4}
        paddingBottom={4}
        width="85%"
        justifyContent="center"
      >
        <Grid>
          <Card
            raised={false}
            sx={{
              width: 440,
              height: "100%",
              display: "flex",
              flexDirection: "column",
              [theme.breakpoints.down("sm")]: {
                width: 300,
              },
            }}
          >
            <CardMedia
              sx={{ width: "100%", height: 200, objectFit: "cover", padding: 0 }}
              component="img"
              src={require(`@site/static/img/tsc_screen.png`).default}
              title="TSC meeting"
            />
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h6" color="text.secondary">
                TSC Meetings
              </Typography>
              <Typography sx={{ my: 1 }} variant="body2" color="text.secondary">
                Every third Wednesday, 9:30-10:30am PT
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Review releases, hear about new features, and discuss the roadmap. Open to all!
              </Typography>
            </CardContent>
            <CardActions disableSpacing>
              <Button
                size="small"
                href="https://zoom-lfx.platform.linuxfoundation.org/meeting/91792261572?password=7c4c7552-0970-480f-9bdb-0b85257879ac"
              >
                Zoom
              </Button>
              <Button sx={{ marginLeft: 2 }} size="small" href="https://www.youtube.com/@openlineageproject6897/videos">
                YouTube
              </Button>
              <Button
                sx={{ marginLeft: 2 }}
                size="small"
                href="https://wiki.lfaidata.foundation/display/OpenLineage/Monthly+TSC+meeting"
              >
                Minutes
              </Button>
            </CardActions>
          </Card>
        </Grid>
        <Grid>
          <Card
            raised={false}
            sx={{
              width: 440,
              height: "100%",
              display: "flex",
              flexDirection: "column",
              [theme.breakpoints.down("sm")]: {
                width: 300,
              },
            }}
          >
            <CardMedia
              sx={{
                width: "100%",
                height: 200,
                objectFit: "contain",
                padding: 2,
                [theme.breakpoints.down("sm")]: {
                  padding: 2,
                },
              }}
              component="img"
              src={require(`@site/static/img/slack.png`).default}
              title="Slack"
            />
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h6" color="text.secondary">
                OpenLineage Slack
              </Typography>
              <Typography sx={{ my: 1 }} variant="body2" color="text.secondary">
                Connect with the community
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Get help from experts, learn about releases and events, and sync up with contributors.
              </Typography>
            </CardContent>
            <CardActions disableSpacing>
              <Button
                size="small"
                href="https://join.slack.com/t/openlineage/shared_invite/zt-3arpql6lg-Nt~hicnDsnDY_GK_LEX06w"
              >
                Join Slack
              </Button>
            </CardActions>
          </Card>
        </Grid>
        <Grid>
          <Card
            raised={false}
            sx={{
              width: 440,
              height: "100%",
              display: "flex",
              flexDirection: "column",
              [theme.breakpoints.down("sm")]: {
                width: 300,
              },
            }}
          >
            <CardMedia
              sx={{
                width: "100%",
                height: 200,
                objectFit: "contain",
                padding: 2,
                [theme.breakpoints.down("sm")]: {
                  padding: 2,
                },
              }}
              component="img"
              src={require(`@site/static/img/github.png`).default}
              title="GitHub"
            />
            <CardContent sx={{ flexGrow: 1 }}>
              <Typography variant="h6" color="text.secondary">
                GitHub Organization
              </Typography>
              <Typography sx={{ my: 1 }} variant="body2" color="text.secondary">
                Contribute to the project
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Access the main codebase, website repo, and workshops. Contributions welcome!
              </Typography>
            </CardContent>
            <CardActions disableSpacing>
              <Button size="small" href="https://github.com/OpenLineage/OpenLineage/CONTRIBUTING.md">
                Contribute
              </Button>
              <Button sx={{ marginLeft: 2 }} size="small" href="https://github.com/OpenLineage/">
                GitHub
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>

      <div className="text-center">
        <h2 className="text-4xl text-color-1">Talks</h2>
      </div>
      <div className="eco-grid-div">
        {FillTalksGrid(
          Talks.sort((a, b) => new Date(b.date).toISOString().localeCompare(new Date(a.date).toISOString())),
        )}
      </div>

      <div className="text-center">
        <h2 className="text-4xl text-color-1">Meetup Groups</h2>
      </div>
      <div className="eco-grid-div">{FillMeetupsGrid(Meetups)}</div>

      <div className="bg-bg">
        <Footer />
      </div>
    </Layout>
  );
}
