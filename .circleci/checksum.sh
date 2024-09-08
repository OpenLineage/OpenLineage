#!/bin/bash
RESULT_FILE=$1
BRANCH_NAME=$2

if [ -f $RESULT_FILE ]; then
  rm $RESULT_FILE
fi
touch $RESULT_FILE

# For dependabot PRs, skip checksum generation to reuse the same cache and reduce storage usage.
if [[ $BRANCH_NAME == dependabot* ]]; then
  echo "DEPENDABOT" >> $RESULT_FILE
  exit 0
fi

checksum_file() {
  echo `openssl md5 $1 | awk '{print $2}'`
}

FILES=()
while read -r -d ''; do
	FILES+=("$REPLY")
done < <(find . -name 'build.gradle' -type f -print0)

# Loop through files and append MD5 to result file
for FILE in ${FILES[@]}; do
	echo `checksum_file $FILE` >> $RESULT_FILE
done
# Now sort the file so that it is
sort $RESULT_FILE -o $RESULT_FILE